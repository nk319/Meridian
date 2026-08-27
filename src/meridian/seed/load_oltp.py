"""Load the generated source data into the OLTP database.

    python -m meridian.seed.load_oltp

This populates the *source system*, not the warehouse. db/init/05_oltp_ddl.sql
creates the tables; without this the OLTP ingestor has nothing to read and
incremental-by-watermark cannot be demonstrated at all.

Deliberately separate from `python -m meridian.seed`, which stays stdlib-only
and writes files. This module talks to Postgres, and importing psycopg from the
generator would make `make seed` depend on a database being up.

Connects as `meridian_app`, the role that owns writes to `oltp` — the same role
the Phase 6 API will use. Loading as a superuser would work and would prove
nothing about whether the application role's grants are right.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import sys
from pathlib import Path

from ..db import UpstreamUnavailable, connect
from ..runlog import EXIT_ERROR, EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE, RunLogger
from ..settings import settings

# Order matters: every table here has a foreign key into one above it, and
# Postgres will reject an order whose customer does not exist yet.
CSV_TABLES = [
    ("customers", "oltp/customers.csv"),
    ("orders", "oltp/orders.csv"),
    ("order_items", "oltp/order_items.csv"),
    ("customer_change_log", "oltp/customer_change_log.csv"),
]

# Tickets are generated as JSON because that is the shape the REST API serves.
# They live in the source database all the same: the API reads from `oltp`, and
# Phase 6 replaces the file with an HTTP endpoint over these very rows.
TICKET_SOURCE = "restapi/support_tickets.json"
TICKET_COLUMNS = [
    "ticket_id",
    "customer_id",
    "order_id",
    "created_ts",
    "resolved_ts",
    "status",
    "channel",
    "subject",
    "body",
    "intent",
    "priority",
    "sentiment",
]
NULLABLE_TIMESTAMPS = {"resolved_ts"}


# Children first: DELETE respects foreign keys, so the order is not cosmetic.
DELETE_ORDER = (
    "support_tickets",
    "customer_change_log",
    "order_items",
    "orders",
    "customers",
)


def clear_all(conn) -> None:
    """Empty the source tables, in one transaction.

    DELETE rather than TRUNCATE, and that is the grants working as intended
    rather than an inefficiency to route around. CONTRACTS §1 gives
    `meridian_app` CRUD on `oltp`, and TRUNCATE is not CRUD — it is a separate
    Postgres privilege that SELECT/INSERT/UPDATE/DELETE does not imply. Widening
    the grant to make a fixture faster would quietly hand the application role
    the ability to empty the source system, which is exactly the authority the
    narrow grant exists to withhold.

    One transaction so the foreign keys never observe a half-cleared state.
    """
    with conn.cursor() as cur:
        for table in DELETE_ORDER:
            cur.execute(f"DELETE FROM {table}")
    conn.commit()


def copy_csv(conn, table: str, path: Path) -> int:
    with path.open(encoding="utf-8", newline="") as fh:
        header = next(csv.reader(fh))
        fh.seek(0)
        columns = ", ".join(f'"{c}"' for c in header)
        with (
            conn.cursor() as cur,
            cur.copy(f"COPY {table} ({columns}) FROM STDIN (FORMAT CSV, HEADER true)") as cp,
        ):
            while chunk := fh.read(1 << 20):
                cp.write(chunk)
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


def copy_tickets(conn, path: Path) -> int:
    rows = json.loads(path.read_text(encoding="utf-8"))
    columns = ", ".join(TICKET_COLUMNS)
    with conn.cursor() as cur, cur.copy(f"COPY support_tickets ({columns}) FROM STDIN") as cp:
        for row in rows:
            cp.write_row(
                tuple(
                    # The generator writes "" for an unresolved ticket. Postgres
                    # will not read that as a timestamp, and coercing it to the
                    # epoch would invent a resolution that never happened.
                    None if (c in NULLABLE_TIMESTAMPS and not row[c]) else row[c]
                    for c in TICKET_COLUMNS
                )
            )
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(
        prog="meridian.seed.load_oltp",
        description="Load generated source data into the oltp database",
    )
    p.add_argument(
        "--keep",
        action="store_true",
        help="append instead of truncating first (will violate primary keys on "
        "a second run; exists for testing the failure)",
    )
    args = p.parse_args(argv)

    cfg = settings()
    log = RunLogger("seed.load_oltp")
    log.emit("start", seeds=str(cfg.seeds_dir))

    if not (cfg.seeds_dir / TICKET_SOURCE).is_file():
        raise FileNotFoundError(f"{cfg.seeds_dir} has no generated data. Run `make seed` first.")

    counts: dict[str, int] = {}
    with connect("meridian_app", cfg.oltp_db, vectors=False) as conn:
        if not args.keep:
            clear_all(conn)

        for table, relative in CSV_TABLES:
            with log.timed("copy", entity=table) as extra:
                counts[table] = copy_csv(conn, table, cfg.seeds_dir / relative)
                extra["rows_out"] = counts[table]

        with log.timed("copy", entity="support_tickets") as extra:
            counts["support_tickets"] = copy_tickets(conn, cfg.seeds_dir / TICKET_SOURCE)
            extra["rows_out"] = counts["support_tickets"]

        conn.commit()

        # Watermark sanity: an ingestor reading `updated_at > x` gets nothing at
        # all if the source timestamps are not what the generator intended, and
        # "no new rows" is indistinguishable from a working incremental run.
        with conn.cursor() as cur:
            cur.execute("SELECT min(order_ts), max(order_ts) FROM orders")
            lo, hi = cur.fetchone()

    log.emit(
        "done",
        rows_out=sum(counts.values()),
        order_ts_min=lo.isoformat() if isinstance(lo, dt.datetime) else lo,
        order_ts_max=hi.isoformat() if isinstance(hi, dt.datetime) else hi,
        **counts,
    )
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
