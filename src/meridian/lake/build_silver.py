"""Bronze → Silver: dedup, type, validate, quarantine.

    python -m meridian.lake.build_silver --entity orders
    python -m meridian.lake.build_silver            # every entity

Silver is where the platform stops repeating what it was told and starts
asserting what it believes. Three things happen, in this order, and the order
matters:

1. **Dedup on `_record_hash`.** Bronze is append-only, so a re-run captures rows
   it already holds, and the generator injects exact duplicate rows on top of
   that. Both collapse here.
2. **Type and validate.** Every column is cast, and a row that fails any rule is
   diverted rather than dropped. A dropped row is indistinguishable from a row
   that never arrived; a quarantined row carries the reason it was rejected.
3. **Keep one version per natural key.** After dedup by hash there can still be
   two genuinely different versions of the same customer, because the customer
   changed. The most recent by business timestamp wins.

Silver is rebuilt in full from Bronze every time. That is not a limitation to
apologise for: Bronze is the append-only record, so a full rebuild is the
definition of idempotent, and a Silver that could drift from its own source
would be worse than one that takes two seconds longer to compute.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import uuid
from dataclasses import dataclass

import duckdb
import yaml

from ..db import UpstreamUnavailable, connect
from ..runlog import (
    EXIT_DQ_BLOCK,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_UPSTREAM_UNAVAILABLE,
    RunLogger,
)
from ..settings import Settings, project_root, settings
from .duck import connect as duck_connect
from .layout import bronze_glob, quarantine_file, silver_current
from .silver_spec import SPECS, Column, SilverSpec

# A feed this broken is not a data-quality finding, it is an outage. Above this
# share of rejected rows the step exits 2 (DQ BLOCK) instead of quietly writing
# a Silver table that is missing most of its rows — which downstream reads as a
# business collapse rather than a pipeline failure.
DEFAULT_MAX_QUARANTINE_PCT = 10.0

OWNER_TEAM = "data-platform"


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------


def _norm(name: str) -> str:
    """Trim, and treat the empty string as absent.

    The file and API feeds are captured as text and use "" for a missing value —
    the generator blanks required fields that way, and unresolved tickets carry
    an empty resolved_ts. Casting "" would produce a type error for a value that
    is really just absent, so the two are distinguished here rather than in
    eight per-entity transforms.
    """
    return f"nullif(trim(CAST({name} AS VARCHAR)), '')"


def _typed(col: Column) -> str:
    return f"TRY_CAST({_norm(col.name)} AS {col.sql_type})"


def quarantine_reason_sql(spec: SilverSpec) -> str:
    """First failing rule per row, as a `rule:column` string.

    Returning the first rather than all of them keeps the expression a single
    CASE and the result a single value the dashboard can group by. A row with
    two problems is still a row to fix; naming one of them is enough to find it.
    """
    branches: list[str] = []
    for col in spec.columns:
        norm, typed = _norm(col.name), _typed(col)
        if not col.nullable:
            branches.append(f"WHEN {norm} IS NULL THEN 'missing:{col.name}'")
        branches.append(f"WHEN {norm} IS NOT NULL AND {typed} IS NULL THEN 'bad_type:{col.name}'")
        if col.enum:
            allowed = ", ".join(f"'{v}'" for v in col.enum)
            branches.append(
                f"WHEN {typed} IS NOT NULL AND {typed} NOT IN ({allowed}) "
                f"THEN 'bad_enum:{col.name}'"
            )
        if col.minimum is not None:
            branches.append(
                f"WHEN {typed} IS NOT NULL AND {typed} < {col.minimum} "
                f"THEN 'out_of_range:{col.name}'"
            )
    return "CASE " + " ".join(branches) + " ELSE NULL END"


def judged_cte(spec: SilverSpec, glob: str) -> str:
    """Bronze, deduped by hash, with a verdict attached to every row."""
    return f"""
    WITH bronze AS (
        SELECT * FROM read_parquet({glob})
    ),
    dedup_hash AS (
        SELECT * EXCLUDE (_rn) FROM (
            SELECT *, row_number() OVER (
                PARTITION BY _record_hash
                ORDER BY _ingested_at DESC, _batch_seq DESC
            ) AS _rn
            FROM bronze
        ) WHERE _rn = 1
    ),
    judged AS (
        SELECT *, {quarantine_reason_sql(spec)} AS _quarantine_reason
        FROM dedup_hash
    )
    """


def good_rows_sql(spec: SilverSpec, glob: str) -> str:
    casts = ",\n            ".join(f"{_typed(c)} AS {c.name}" for c in spec.columns)
    recency = spec.recency
    if recency != "_ingested_at":
        recency = f"TRY_CAST({_norm(recency)} AS TIMESTAMPTZ)"
    key = ", ".join(spec.key)
    return f"""
    {judged_cte(spec, glob)}
    , ranked AS (
        SELECT *, row_number() OVER (
            PARTITION BY {key}
            -- NULLS LAST so a row with an unparseable business timestamp never
            -- outranks a good one for the same key.
            ORDER BY {recency} DESC NULLS LAST, _ingested_at DESC, _batch_seq DESC
        ) AS _rank
        FROM judged
        WHERE _quarantine_reason IS NULL
    )
    SELECT
            {casts},
            _source_system,
            CAST(_ingest_run_id AS UUID) AS _ingest_run_id,
            _ingested_at,
            _record_hash
    FROM ranked WHERE _rank = 1
    """


def quarantine_rows_sql(spec: SilverSpec, glob: str) -> str:
    """Rejected rows, with their ORIGINAL values.

    Deliberately not the cast values: the whole reason a row is here is that a
    cast failed, and a quarantine table full of NULLs where the bad data used to
    be tells you nothing about what the source actually sent.
    """
    return f"{judged_cte(spec, glob)} SELECT * FROM judged WHERE _quarantine_reason IS NOT NULL"


# ---------------------------------------------------------------------------
# PII guard
# ---------------------------------------------------------------------------


def restricted_column_names(path=None) -> set[str]:
    path = path or (project_root() / "docs" / "governance" / "pii_classification.yml")
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {
        entry["column"] for entry in doc.get("columns", []) if entry.get("level") == "restricted"
    }


def assert_no_restricted_columns(
    con: duckdb.DuckDBPyConnection, spec: SilverSpec, glob: str
) -> None:
    """CONTRACTS §3 lists "PII scrub" as part of this hop.

    On this pipeline the scrub is a no-op, because the generator splits names,
    emails and phone numbers out at generation time and they never enter the
    lake. A no-op is exactly the kind of guarantee that quietly stops being true
    — someone adds a column to the source and nothing complains. So instead of
    scrubbing nothing, this asserts that there was nothing to scrub, reading the
    restricted list from the governance file rather than a hardcoded copy.
    """
    present = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({glob})").fetchall()}
    leaked = present & restricted_column_names()
    if leaked:
        raise RuntimeError(
            f"{spec.entity}: Bronze carries restricted PII columns {sorted(leaked)}. "
            f"Restricted data must never enter the lake — it is split into "
            f"secure.customer_pii at generation time (CONTRACTS.md §10). Refusing "
            f"to build Silver."
        )


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


@dataclass
class EntityResult:
    entity: str
    bronze_rows: int
    deduped_rows: int
    quarantined: int
    silver_rows: int
    reasons: dict[str, int]
    silver_path: str
    quarantine_path: str | None

    @property
    def quarantine_pct(self) -> float:
        return 100.0 * self.quarantined / self.deduped_rows if self.deduped_rows else 0.0


def build_entity(
    con: duckdb.DuckDBPyConnection, spec: SilverSpec, cfg: Settings, run_id: uuid.UUID
) -> EntityResult:
    # Every Bronze source that carries this entity, minus the ones with nothing
    # in them. Filtering is not optional: `read_parquet` on a glob matching no
    # file is an error, so an entity whose streaming sink has never run would
    # fail to build at all — and the streaming path must be optional.
    present = [
        candidate
        for candidate in (
            bronze_glob(cfg.lake_bucket, source, spec.entity) for source in spec.all_sources
        )
        if con.execute(f"SELECT count(*) FROM glob('{candidate}')").fetchone()[0]
    ]
    if not present:
        raise FileNotFoundError(
            f"no Bronze for {spec.entity} under any of {list(spec.all_sources)}. Run "
            f"`python -m meridian.ingest.{spec.source} --mode full` first."
        )

    # A DuckDB list literal when there is more than one, a quoted string when
    # there is one. `read_parquet` accepts both, and keeping the single-source
    # case a plain string keeps the generated SQL readable in a log.
    glob = (
        f"'{present[0]}'"
        if len(present) == 1
        else "[" + ", ".join(f"'{path}'" for path in present) + "]"
    )

    assert_no_restricted_columns(con, spec, glob)

    bronze_rows = con.execute(f"SELECT count(*) FROM read_parquet({glob})").fetchone()[0]
    counts = con.execute(
        f"{judged_cte(spec, glob)} "
        f"SELECT count(*), count(*) FILTER (WHERE _quarantine_reason IS NOT NULL) FROM judged"
    ).fetchone()
    deduped_rows, quarantined = counts

    reasons = dict(
        con.execute(
            f"{judged_cte(spec, glob)} "
            f"SELECT _quarantine_reason, count(*) FROM judged "
            f"WHERE _quarantine_reason IS NOT NULL GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
    )

    silver_path = silver_current(cfg.lake_bucket, spec.entity)
    con.execute(
        f"COPY ({good_rows_sql(spec, glob)}) TO '{silver_path}' (FORMAT PARQUET, COMPRESSION zstd)"
    )
    silver_rows = con.execute(f"SELECT count(*) FROM read_parquet('{silver_path}')").fetchone()[0]

    quarantine_path = None
    if quarantined:
        quarantine_path = quarantine_file(
            cfg.lake_bucket, spec.entity, dt.datetime.now(dt.UTC).date(), str(run_id)
        )
        con.execute(
            f"COPY ({quarantine_rows_sql(spec, glob)}) TO '{quarantine_path}' "
            f"(FORMAT PARQUET, COMPRESSION zstd)"
        )

    return EntityResult(
        entity=spec.entity,
        bronze_rows=bronze_rows,
        deduped_rows=deduped_rows,
        quarantined=quarantined,
        silver_rows=silver_rows,
        reasons=reasons,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )


# ---------------------------------------------------------------------------
# data quality records
# ---------------------------------------------------------------------------


def record_dq(pg, run_id: uuid.UUID, result: EntityResult, max_pct: float) -> None:
    """Write one meta.dq_check_results row per rule, plus the threshold check.

    Row-level rejections are QUARANTINE: the row is gone but the pipeline is
    healthy. The threshold check is BLOCK, because a feed that is mostly
    rejected is not a quality finding — it is an outage wearing one.
    """
    rows = []
    target = f"silver.{result.entity}"

    for reason, count in result.reasons.items():
        rule, _, column = reason.partition(":")
        rows.append(
            (
                uuid.uuid4(),
                run_id,
                "custom",
                "silver_build",
                rule,
                target,
                column or None,
                "QUARANTINE",
                "FAIL",
                result.deduped_rows,
                count,
                round(100.0 * count / result.deduped_rows, 4) if result.deduped_rows else 0,
                OWNER_TEAM,
                f"SELECT * FROM read_parquet('{result.quarantine_path}') "
                f"WHERE _quarantine_reason = '{reason}'",
                f"{count} rows failed {rule} on {column}",
            )
        )

    duplicates = result.bronze_rows - result.deduped_rows
    rows.append(
        (
            uuid.uuid4(),
            run_id,
            "custom",
            "silver_build",
            "duplicate_rows",
            target,
            None,
            "WARN",
            "FAIL" if duplicates else "PASS",
            result.bronze_rows,
            duplicates,
            round(100.0 * duplicates / result.bronze_rows, 4) if result.bronze_rows else 0,
            OWNER_TEAM,
            None,
            f"{duplicates} Bronze rows collapsed on _record_hash",
        )
    )

    breached = result.quarantine_pct > max_pct
    rows.append(
        (
            uuid.uuid4(),
            run_id,
            "custom",
            "silver_build",
            "quarantine_rate",
            target,
            None,
            "BLOCK",
            "FAIL" if breached else "PASS",
            result.deduped_rows,
            result.quarantined,
            round(result.quarantine_pct, 4),
            OWNER_TEAM,
            None,
            f"{result.quarantine_pct:.2f}% quarantined against a {max_pct:.2f}% ceiling",
        )
    )

    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO meta.dq_check_results (check_run_id, run_id, source, suite, "
            "check_name, target_table, target_column, severity, status, rows_scanned, "
            "rows_failed, failure_pct, owner_team, repro_sql, message) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            rows,
        )
    pg.commit()


# ---------------------------------------------------------------------------
# entrypoint
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="meridian.lake.build_silver",
        description="Rebuild Silver from Bronze: dedup, type, validate, quarantine",
    )
    p.add_argument("--entity", action="append", choices=sorted(SPECS), default=None)
    p.add_argument("--max-quarantine-pct", type=float, default=DEFAULT_MAX_QUARANTINE_PCT)
    args = p.parse_args(argv)

    cfg = settings()
    specs = [SPECS[e] for e in (args.entity or sorted(SPECS))]
    log = RunLogger("lake.build_silver")
    run_id = uuid.UUID(log.run_id)
    started = dt.datetime.now(dt.UTC)
    log.emit(
        "start", entities=[s.entity for s in specs], max_quarantine_pct=args.max_quarantine_pct
    )

    con = duck_connect(cfg)
    blocked: list[str] = []
    with connect("meridian_etl", vectors=False) as pg:
        with pg.cursor() as cur:
            cur.execute(
                "INSERT INTO meta.pipeline_run_log (run_id, step, started_at, status) "
                "VALUES (%s, 'lake.build_silver', %s, 'RUNNING')",
                (run_id, started),
            )
        pg.commit()

        total_in = total_out = 0
        try:
            for spec in specs:
                with log.timed("build", entity=spec.entity) as extra:
                    result = build_entity(con, spec, cfg, run_id)
                    record_dq(pg, run_id, result, args.max_quarantine_pct)
                    extra.update(
                        rows_in=result.bronze_rows,
                        rows_out=result.silver_rows,
                        deduped=result.bronze_rows - result.deduped_rows,
                        quarantined=result.quarantined,
                        quarantine_pct=round(result.quarantine_pct, 3),
                        reasons=result.reasons or None,
                    )
                total_in += result.bronze_rows
                total_out += result.silver_rows
                if result.quarantine_pct > args.max_quarantine_pct:
                    blocked.append(f"{spec.entity} {result.quarantine_pct:.2f}%")
        except Exception:
            # Without this the run stays RUNNING forever. An ops dashboard reads
            # that as "still in flight", so a crashed build is indistinguishable
            # from a slow one — and `meta.pipeline_run_log.completed_at` is the
            # dashboard's cache key (§7), so it would also keep serving stale
            # numbers as if they were fresh.
            pg.rollback()
            with pg.cursor() as cur:
                cur.execute(
                    "UPDATE meta.pipeline_run_log SET completed_at = now(), "
                    "status = 'FAILED' WHERE run_id = %s",
                    (run_id,),
                )
            pg.commit()
            raise

        status = "FAILED" if blocked else "SUCCESS"
        with pg.cursor() as cur:
            cur.execute(
                "UPDATE meta.pipeline_run_log SET completed_at = now(), status = %s, "
                "rows_in = %s, rows_out = %s WHERE run_id = %s",
                (status, total_in, total_out, run_id),
            )
        pg.commit()

    if blocked:
        log.emit("dq_block", entities=blocked, max_quarantine_pct=args.max_quarantine_pct)
        return EXIT_DQ_BLOCK

    log.emit("done", rows_in=total_in, rows_out=total_out)
    return EXIT_OK


def cli() -> int:
    try:
        return main()
    except UpstreamUnavailable as exc:
        print(json.dumps({"event": "upstream_unavailable", "error": str(exc)}))
        return EXIT_UPSTREAM_UNAVAILABLE
    except Exception as exc:  # noqa: BLE001 - top-level boundary
        print(json.dumps({"event": "error", "error": f"{type(exc).__name__}: {exc}"}))
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(cli())
