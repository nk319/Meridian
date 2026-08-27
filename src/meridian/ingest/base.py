"""Shared scaffolding for the four batch ingestion sources.

Each source differs only in how it reaches its data — a Postgres attach, a CSV
drop, a JSON document, a paginated cursor walk. Everything after that is
identical: read a watermark, capture raw rows into Bronze, advance the
watermark, record the run. That common half lives here so a fix to it is one
fix, not four.

The rule the seed generator's docstring exists to protect: **each entity has
exactly one ingestion owner**. An earlier design had orders arriving over four
paths at once, which made Bronze row-count reconciliation fail permanently and
made every pipeline run look broken. `OWNERSHIP` below is that rule written
down, and `check_ownership` is it enforced.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass

import duckdb

from ..db import UpstreamUnavailable, connect
from ..lake import bronze
from ..lake.duck import connect as duck_connect
from ..lake.layout import bronze_glob
from ..runlog import EXIT_ERROR, EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE, RunLogger
from ..settings import Settings, settings

# Entity -> the one source system allowed to ingest it. CONTRACTS.md §5.
OWNERSHIP = {
    "customers": "oltp",
    "orders": "oltp",
    "order_items": "oltp",
    "customer_change_log": "oltp",
    "products": "files",
    "web_events": "files",
    "support_tickets": "restapi",
    "payments": "vendor",
}


@dataclass(frozen=True)
class Extract:
    """One unit of raw capture: a SELECT and where it came from."""

    select_sql: str
    source_file: str


@dataclass(frozen=True)
class EntitySpec:
    entity: str
    business_columns: tuple[str, ...]
    # None means the entity has no usable event timestamp and is full-refresh
    # only. Saying so explicitly beats a silently ignored --mode incremental.
    watermark_column: str | None
    build: Callable[[duckdb.DuckDBPyConnection, Settings, dt.datetime | None], Iterable[Extract]]
    order_by: str | None = None


def check_ownership(source: str, entity: str) -> None:
    owner = OWNERSHIP.get(entity)
    if owner is None:
        raise ValueError(f"{entity!r} has no declared ingestion owner in OWNERSHIP")
    if owner != source:
        raise ValueError(
            f"{entity!r} is ingested from {owner!r}, not {source!r}. Two paths for "
            f"one entity double-count it in Bronze and make the reconciliation "
            f"check fail permanently (CONTRACTS.md §5)."
        )


# ---------------------------------------------------------------------------
# watermarks
# ---------------------------------------------------------------------------


def read_watermark(conn, entity: str) -> dt.datetime | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT watermark_value FROM meta.ingest_watermarks WHERE entity = %s",
            (entity,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def write_watermark(conn, entity: str, value: dt.datetime) -> None:
    """Advance a watermark, never retreat it.

    The GREATEST guard matters when a full refresh runs after an incremental
    one: a full pass legitimately re-reads old rows, and taking its maximum
    unconditionally would be a no-op — but a *filtered* re-run, or two runs
    racing, could otherwise move the watermark backwards and cause the next
    incremental pass to re-ingest a window that was already captured.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO meta.ingest_watermarks (entity, watermark_value, updated_at) "
            "VALUES (%s, %s, now()) "
            "ON CONFLICT (entity) DO UPDATE SET "
            "  watermark_value = GREATEST(meta.ingest_watermarks.watermark_value, EXCLUDED.watermark_value), "
            "  updated_at = now()",
            (entity, value),
        )
    conn.commit()


def bronze_has_data(con: duckdb.DuckDBPyConnection, glob: str) -> bool:
    """Whether this entity has any Bronze yet. First run has none."""
    return bool(con.execute(f"SELECT count(*) FROM glob('{glob}')").fetchone()[0])


def incremental_where(
    con: duckdb.DuckDBPyConnection,
    cfg: Settings,
    *,
    source: str,
    entity: str,
    ts_column: str,
    watermark: dt.datetime | None,
    business_columns: tuple[str, ...],
) -> str:
    """The WHERE clause for an incremental pass over a text-typed feed.

    Two things have to be true at once, and getting either wrong is invisible.

    A row whose timestamp does not parse — the generator injects "2026-13-45"
    and "not-a-date" — compares NULL against the watermark, so a plain
    `ts > watermark` silently excludes every corrupted row. The pipeline then
    looks clean precisely because the bad data disappeared before anything could
    count it, which is the worst available outcome.

    But simply adding `OR ts IS NULL` re-captures every malformed row on every
    run, forever: they never become "old" because they have no timestamp to
    compare. Measured on this corpus that was 905 web events and 74 payments
    re-ingested per pass, growing Bronze without bound and re-doing work that
    was already done.

    So unparseable rows are admitted exactly once, by anti-joining on the record
    hash against what Bronze already holds. They are never dropped, and never
    captured twice.
    """
    if watermark is None:
        return ""

    cast = f"TRY_CAST({ts_column} AS TIMESTAMPTZ)"
    clauses = [f"{cast} > TIMESTAMPTZ '{watermark.isoformat()}'"]

    glob = bronze_glob(cfg.lake_bucket, source, entity)
    if bronze_has_data(con, glob):
        clauses.append(
            f"({cast} IS NULL AND {bronze.record_hash_expr(business_columns)} "
            f"NOT IN (SELECT _record_hash FROM read_parquet('{glob}')))"
        )
    else:
        clauses.append(f"{cast} IS NULL")

    return "WHERE " + " OR ".join(clauses)


# ---------------------------------------------------------------------------
# run log
# ---------------------------------------------------------------------------


def open_run(conn, run_id: uuid.UUID, step: str, started: dt.datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO meta.pipeline_run_log (run_id, step, started_at, status) "
            "VALUES (%s, %s, %s, 'RUNNING')",
            (run_id, step, started),
        )
    conn.commit()


def close_run(conn, run_id: uuid.UUID, status: str, rows_in: int, rows_out: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE meta.pipeline_run_log SET completed_at = now(), status = %s, "
            "rows_in = %s, rows_out = %s WHERE run_id = %s",
            (status, rows_in, rows_out, run_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# one entity
# ---------------------------------------------------------------------------


@dataclass
class IngestResult:
    entity: str
    source: str
    mode: str
    rows: int
    files: list[str]
    watermark_before: dt.datetime | None
    watermark_after: dt.datetime | None


def ingest_entity(
    con: duckdb.DuckDBPyConnection,
    pg,
    spec: EntitySpec,
    source: str,
    mode: str,
    log: RunLogger,
    cfg: Settings,
) -> IngestResult:
    check_ownership(source, spec.entity)

    if mode == "incremental" and spec.watermark_column is None:
        raise ValueError(
            f"{spec.entity} has no watermark column, so --mode incremental would "
            f"silently re-read everything. Use --mode full."
        )

    run_id = uuid.uuid4()
    started = dt.datetime.now(dt.UTC)
    step = f"ingest.{source}.{spec.entity}"
    open_run(pg, run_id, step, started)

    watermark = read_watermark(pg, spec.entity) if mode == "incremental" else None
    rows = 0
    files: list[str] = []
    try:
        for seq, extract in enumerate(spec.build(con, cfg, watermark)):
            written = bronze.write(
                con,
                source=source,
                entity=spec.entity,
                select_sql=extract.select_sql,
                business_columns=spec.business_columns,
                run_id=str(run_id),
                source_file=extract.source_file,
                seq=seq,
                batch_offset=rows,
                order_by=spec.order_by,
                cfg=cfg,
            )
            rows += written.rows
            files.append(written.path)
            log.emit(
                "bronze_part",
                entity=spec.entity,
                seq=seq,
                rows_out=written.rows,
                path=written.path,
                source_file=extract.source_file,
            )
    except Exception:
        # Roll back before recording: if the failure came from Postgres the
        # transaction is aborted, and close_run would then raise over the top of
        # the real error while leaving the run marked RUNNING.
        pg.rollback()
        close_run(pg, run_id, "FAILED", 0, rows)
        raise

    # Advance from what actually landed, read back out of the Bronze files
    # rather than from the extract query. If a cast or a filter dropped rows on
    # the way in, the watermark must reflect the data that exists, not the data
    # that was asked for — otherwise the gap is skipped forever.
    new_watermark = None
    if spec.watermark_column and files:
        paths = ", ".join(f"'{p}'" for p in files)
        # Fetched as text and parsed here rather than as a datetime. Handing a
        # TIMESTAMP WITH TIME ZONE back to Python makes DuckDB import pytz,
        # which is not one of its install-time requirements — the failure is an
        # ImportError from inside the query engine, several layers from anything
        # that mentions time zones. A cast and fromisoformat cost nothing, carry
        # full microsecond precision, and remove the dependency entirely.
        rendered = con.execute(
            f"SELECT CAST(max(TRY_CAST({spec.watermark_column} AS TIMESTAMPTZ)) AS VARCHAR) "
            f"FROM read_parquet([{paths}])"
        ).fetchone()[0]
        if rendered:
            new_watermark = dt.datetime.fromisoformat(rendered)
            write_watermark(pg, spec.entity, new_watermark)

    close_run(pg, run_id, "SUCCESS", rows, rows)
    return IngestResult(
        entity=spec.entity,
        source=source,
        mode=mode,
        rows=rows,
        files=files,
        watermark_before=watermark,
        watermark_after=new_watermark,
    )


# ---------------------------------------------------------------------------
# CLI shared by all four sources
# ---------------------------------------------------------------------------


def build_parser(
    source: str, entities: list[str], modes: tuple[str, ...]
) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog=f"meridian.ingest.{source}",
        description=f"Capture the {source} source into Bronze",
    )
    p.add_argument("--mode", choices=modes, default=modes[0])
    p.add_argument(
        "--entity",
        action="append",
        choices=entities,
        default=None,
        help="repeatable; defaults to every entity this source owns",
    )
    return p


def run_source(
    source: str,
    specs: list[EntitySpec],
    argv: list[str] | None = None,
    modes: tuple[str, ...] = ("full", "incremental"),
    setup: Callable[[duckdb.DuckDBPyConnection, Settings], None] | None = None,
) -> int:
    by_name = {s.entity: s for s in specs}
    args = build_parser(source, sorted(by_name), modes).parse_args(argv)
    chosen = [by_name[e] for e in (args.entity or sorted(by_name))]

    cfg = settings()
    log = RunLogger(f"ingest.{source}")
    log.emit("start", source=source, mode=args.mode, entities=[s.entity for s in chosen])

    con = duck_connect(cfg)
    if setup is not None:
        setup(con, cfg)

    totals = 0
    with connect("meridian_etl", vectors=False) as pg:
        for spec in chosen:
            with log.timed("ingest", entity=spec.entity) as extra:
                result = ingest_entity(con, pg, spec, source, args.mode, log, cfg)
                extra["rows_out"] = result.rows
                extra["files"] = len(result.files)
                extra["watermark_before"] = (
                    result.watermark_before.isoformat() if result.watermark_before else None
                )
                extra["watermark_after"] = (
                    result.watermark_after.isoformat() if result.watermark_after else None
                )
            totals += result.rows

    log.emit("done", source=source, mode=args.mode, rows_out=totals)
    return EXIT_OK


def cli(fn: Callable[[], int]) -> int:
    try:
        return fn()
    except UpstreamUnavailable as exc:
        print(json.dumps({"event": "upstream_unavailable", "error": str(exc)}))
        return EXIT_UPSTREAM_UNAVAILABLE
    except Exception as exc:  # noqa: BLE001 - top-level boundary, exit code is the contract
        print(json.dumps({"event": "error", "error": f"{type(exc).__name__}: {exc}"}))
        return EXIT_ERROR


def exit_with(fn: Callable[[], int]) -> None:
    sys.exit(cli(fn))
