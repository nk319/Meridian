"""Capture support tickets from the platform's own REST API into Bronze.

    python -m meridian.ingest.restapi --mode incremental

Phase 2 reads the JSON document the API will serve; Phase 6 builds the endpoint
that serves it. The transport is the only thing that changes — the columns, the
watermark, the record hash and the Bronze path are all fixed here, so swapping
`read_json(file)` for an HTTP page walk does not touch anything downstream.

Read as VARCHAR. DuckDB's inference on this document types `created_ts` as a
timestamp and `resolved_ts` as text, because unresolved tickets carry an empty
string — so inference alone produces two different treatments of the same
concept. Declaring every column VARCHAR makes Bronze a faithful copy and leaves
the decision about what an empty string means to Silver, which is where the
answer ("NULL, and the ticket is still open") actually belongs.
"""

from __future__ import annotations

from .base import EntitySpec, Extract, exit_with, incremental_where, run_source

SOURCE = "restapi"

TICKET_COLUMNS = (
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
)


def _tickets(con, cfg, watermark):
    path = cfg.seeds_dir / "restapi" / "support_tickets.json"
    columns = ", ".join(f"'{c}': 'VARCHAR'" for c in TICKET_COLUMNS)
    where = incremental_where(
        con,
        cfg,
        source=SOURCE,
        entity="support_tickets",
        ts_column="created_ts",
        watermark=watermark,
        business_columns=TICKET_COLUMNS,
    )
    yield Extract(
        select_sql=(
            f"SELECT {', '.join(TICKET_COLUMNS)} "
            f"FROM read_json('{path}', columns={{{columns}}}) {where}"
        ),
        source_file="restapi:/v1/support/tickets",
    )


SPECS = [EntitySpec("support_tickets", TICKET_COLUMNS, "created_ts", _tickets)]


def main() -> int:
    return run_source(SOURCE, SPECS)


if __name__ == "__main__":
    exit_with(main)
