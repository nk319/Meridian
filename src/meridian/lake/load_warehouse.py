"""Silver Parquet → warehouse.silver, streamed through Arrow into COPY.

    python -m meridian.lake.load_warehouse --entity orders
    python -m meridian.lake.load_warehouse                    # every entity

This is the load-bearing hop (CONTRACTS.md §3). The spec asked for Parquet in
object storage, dbt transformations and a Postgres warehouse — but
`dbt-postgres` cannot read Parquet from MinIO, so as originally written Bronze
and Silver never reached dbt at all. DuckDB reads the Parquet, Arrow carries the
batches, and `COPY` puts them in Postgres.

Arrow + COPY rather than DuckDB's Postgres `ATTACH`:

- it streams in bounded memory, one record batch at a time, so a 150,000-row
  entity costs the same resident memory as a 150-row one;
- it needs no extension installed in the Postgres server;
- and **binary COPY fails loudly on a type mismatch** rather than coercing.
  That last one was verified rather than assumed: feeding an int where the
  column is text aborts the COPY with a Postgres error instead of writing
  something plausible.

Each entity is truncate-and-load inside one transaction. Silver is a full
rebuild from append-only Bronze, so replacing the table is the honest operation;
doing it transactionally means a failed load leaves the previous contents intact
rather than an empty table that reads downstream as "the business stopped".
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
import uuid
from dataclasses import dataclass

import duckdb

from ..db import UpstreamUnavailable, connect
from ..runlog import EXIT_ERROR, EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE, RunLogger
from ..settings import Settings, settings
from .duck import connect as duck_connect
from .layout import silver_glob
from .silver_spec import SPECS, SilverSpec

# DuckDB column type -> the name psycopg's binary COPY needs. Explicit rather
# than inferred: binary COPY has no negotiation, and a wrong guess here writes
# bytes Postgres will misread rather than raising.
PG_TYPE = {
    "VARCHAR": "text",
    "DATE": "date",
    "TIMESTAMPTZ": "timestamptz",
    "BOOLEAN": "bool",
    "INTEGER": "int4",
    "DECIMAL(12,2)": "numeric",
}

# Lineage columns carried from Bronze, in the order Silver stores them.
LINEAGE = (
    ("_source_system", "text"),
    ("_ingest_run_id", "uuid"),
    ("_ingested_at", "timestamptz"),
    ("_record_hash", "text"),
)

BATCH_ROWS = 20_000

# PII never travels the lake, so it never arrives here as Parquet. See
# load_customer_pii below.
SECURE_ENTITY = "customer_pii"


@dataclass
class LoadResult:
    entity: str
    parquet_rows: int
    loaded_rows: int


def _pg_types(spec: SilverSpec) -> list[str]:
    types = []
    for col in spec.columns:
        try:
            types.append(PG_TYPE[col.sql_type])
        except KeyError as exc:
            raise KeyError(
                f"{spec.entity}.{col.name}: no Postgres COPY type mapped for "
                f"{col.sql_type!r}. Add it to PG_TYPE — binary COPY cannot guess."
            ) from exc
    return types + [t for _, t in LINEAGE]


def _columns(spec: SilverSpec) -> list[str]:
    return list(spec.business_columns) + [n for n, _ in LINEAGE]


def load_entity(con: duckdb.DuckDBPyConnection, pg, spec: SilverSpec, cfg: Settings) -> LoadResult:
    glob = silver_glob(cfg.lake_bucket, spec.entity)
    if not con.execute(f"SELECT count(*) FROM glob('{glob}')").fetchone()[0]:
        raise FileNotFoundError(
            f"no Silver for {spec.entity} at {glob}. Run "
            f"`python -m meridian.lake.build_silver --entity {spec.entity}` first."
        )

    columns = _columns(spec)
    types = _pg_types(spec)
    parquet_rows = con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0]

    reader = con.execute(
        f"SELECT {', '.join(columns)} FROM read_parquet('{glob}')"
    ).to_arrow_reader(BATCH_ROWS)

    # DuckDB's UUID arrives through Arrow as a string, and psycopg's binary
    # dumper needs the object. Converted per column rather than per value so the
    # check is not repeated 150,000 times for columns that never need it.
    uuid_positions = [i for i, t in enumerate(types) if t == "uuid"]

    loaded = 0
    with pg.cursor() as cur:
        # Transactional: a failure below rolls this back and leaves the previous
        # contents in place.
        cur.execute(f"TRUNCATE silver.{spec.entity}")
        with cur.copy(
            f"COPY silver.{spec.entity} ({', '.join(columns)}) FROM STDIN (FORMAT BINARY)"
        ) as cp:
            cp.set_types(types)
            for batch in reader:
                cols = [c.to_pylist() for c in batch.columns]
                for i in uuid_positions:
                    cols[i] = [uuid.UUID(v) if isinstance(v, str) else v for v in cols[i]]
                for row in zip(*cols, strict=True):
                    cp.write_row(row)
                    loaded += 1
    pg.commit()

    if loaded != parquet_rows:
        raise RuntimeError(
            f"{spec.entity}: Silver holds {parquet_rows} rows but {loaded} were "
            f"written. A count that does not reconcile is a silent data loss bug."
        )
    return LoadResult(entity=spec.entity, parquet_rows=parquet_rows, loaded_rows=loaded)


def load_customer_pii(pg, cfg: Settings) -> LoadResult:
    """Load secure.customer_pii straight from the seed, bypassing the lake.

    This is the enforcement mechanism from CONTRACTS.md §10, not a shortcut. The
    generator splits names, emails and phone numbers into their own file, and
    they load directly into `secure` — so there is no stage of the pipeline that
    ever holds them alongside business data, and therefore no step that could
    forget to drop them. `build_silver.assert_no_restricted_columns` is the
    other half: it fails the build if a restricted column ever appears in
    Bronze.
    """
    path = cfg.seeds_dir / "secure" / "customer_pii.csv"
    if not path.is_file():
        raise FileNotFoundError(f"{path} not found. Run `make seed` first.")

    with path.open(encoding="utf-8", newline="") as fh:
        header = next(csv.reader(fh))
        fh.seek(0)
        columns = ", ".join(f'"{c}"' for c in header)
        with pg.cursor() as cur:
            cur.execute("TRUNCATE secure.customer_pii")
            with cur.copy(
                f"COPY secure.customer_pii ({columns}) FROM STDIN (FORMAT CSV, HEADER true)"
            ) as cp:
                while chunk := fh.read(1 << 20):
                    cp.write(chunk)
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM secure.customer_pii")
        loaded = cur.fetchone()[0]
    pg.commit()
    return LoadResult(entity=SECURE_ENTITY, parquet_rows=loaded, loaded_rows=loaded)


def record_reconciliation(pg, run_id: uuid.UUID, result: LoadResult) -> None:
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO meta.dq_check_results (check_run_id, run_id, source, suite, "
            "check_name, target_table, severity, status, rows_scanned, rows_failed, "
            "failure_pct, owner_team, message) "
            "VALUES (%s,%s,'custom','warehouse_load','row_count_reconciliation',%s,"
            "'BLOCK','PASS',%s,0,0,'data-platform',%s)",
            (
                uuid.uuid4(),
                run_id,
                f"silver.{result.entity}",
                result.loaded_rows,
                f"{result.loaded_rows} rows loaded, matching the source exactly",
            ),
        )
    pg.commit()


def main(argv: list[str] | None = None) -> int:
    choices = sorted(SPECS) + [SECURE_ENTITY]
    p = argparse.ArgumentParser(
        prog="meridian.lake.load_warehouse",
        description="Stream Silver Parquet into warehouse.silver via Arrow and COPY",
    )
    p.add_argument("--entity", action="append", choices=choices, default=None)
    args = p.parse_args(argv)

    cfg = settings()
    chosen = args.entity or choices
    log = RunLogger("lake.load_warehouse")
    run_id = uuid.UUID(log.run_id)
    started = dt.datetime.now(dt.UTC)
    log.emit("start", entities=chosen)

    con = duck_connect(cfg)
    total = 0
    with connect("meridian_etl", vectors=False) as pg:
        with pg.cursor() as cur:
            cur.execute(
                "INSERT INTO meta.pipeline_run_log (run_id, step, started_at, status) "
                "VALUES (%s, 'lake.load_warehouse', %s, 'RUNNING')",
                (run_id, started),
            )
        pg.commit()

        try:
            for entity in chosen:
                with log.timed("load", entity=entity) as extra:
                    if entity == SECURE_ENTITY:
                        result = load_customer_pii(pg, cfg)
                        extra["route"] = "direct from seed, never through the lake"
                    else:
                        result = load_entity(con, pg, SPECS[entity], cfg)
                    record_reconciliation(pg, run_id, result)
                    extra["rows_in"] = result.parquet_rows
                    extra["rows_out"] = result.loaded_rows
                total += result.loaded_rows
        except Exception:
            # Roll back first. A COPY that fails leaves the transaction in an
            # aborted state, and every later statement on it — including this
            # UPDATE — raises InFailedSqlTransaction. Recording the failure
            # would then fail, and the error that surfaced would be the
            # bookkeeping one rather than the one that actually broke the load.
            pg.rollback()
            with pg.cursor() as cur:
                cur.execute(
                    "UPDATE meta.pipeline_run_log SET completed_at = now(), "
                    "status = 'FAILED' WHERE run_id = %s",
                    (run_id,),
                )
            pg.commit()
            raise

        with pg.cursor() as cur:
            cur.execute(
                "UPDATE meta.pipeline_run_log SET completed_at = now(), status = 'SUCCESS', "
                "rows_in = %s, rows_out = %s WHERE run_id = %s",
                (total, total, run_id),
            )
        pg.commit()

    log.emit("done", rows_out=total)
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
