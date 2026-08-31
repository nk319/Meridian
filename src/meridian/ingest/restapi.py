"""Capture support tickets from the platform's own REST API into Bronze.

    python -m meridian.ingest.restapi --mode incremental
    MERIDIAN_API_URL=http://localhost:8000 python -m meridian.ingest.restapi --mode incremental

Phase 2 read the JSON document the API would serve; Phase 6 built the endpoint,
and this now reads either. Setting `MERIDIAN_API_URL` walks the live
`GET /v1/support/tickets` feed; leaving it unset reads `seeds/` exactly as
before, so the pipeline still runs with the API switched off.

That the switch is this small is the point Phase 2 was making. The columns, the
watermark, the record hash and the Bronze path were fixed there, and the HTTP
path below ends by writing NDJSON to a temporary file and handing DuckDB the
same `read_json` it always used — so the transport is genuinely the only thing
that differs, and nothing downstream can tell which one ran.

Read as VARCHAR. DuckDB's inference on this document types `created_ts` as a
timestamp and `resolved_ts` as text, because unresolved tickets carry an empty
string — so inference alone produces two different treatments of the same
concept. Declaring every column VARCHAR makes Bronze a faithful copy and leaves
the decision about what an empty string means to Silver, which is where the
answer ("NULL, and the ticket is still open") actually belongs.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .base import EntitySpec, Extract, exit_with, incremental_where, run_source

SOURCE = "restapi"

# How many tickets to ask for per request. Not a tuning knob so much as a
# statement about the API's contract: it caps `limit` at 500, and a client that
# asks for more gets a validation error rather than silently fewer.
PAGE_SIZE = 500

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


def _fetch_pages(base_url: str, watermark) -> list[dict]:
    """Walk the cursor-paginated feed to the end.

    Cursor, not offset. The API pages on `(created_ts, ticket_id)` and returns
    an opaque `next_cursor`; following it is both cheaper for the server and
    the only shape that is correct while rows are being inserted. Offset
    pagination against a table being written to skips and repeats rows, and
    does so silently.

    The watermark is passed as `since`, so an incremental run asks the *server*
    for the new tickets rather than downloading everything and filtering here.
    """
    import httpx

    headers = {}
    key = os.environ.get("API_INGEST_KEY", "")
    token = os.environ.get("MERIDIAN_API_TOKEN", "")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif key:
        # The ingest key carries `tickets:write` only, so it cannot read this
        # feed — deliberately, per api/security.py. Reading needs a token; this
        # branch exists to give a clear error rather than a confusing 401.
        raise RuntimeError(
            "MERIDIAN_API_URL is set but MERIDIAN_API_TOKEN is not. The ingest "
            "key grants tickets:write only and cannot read the feed — obtain a "
            "token from POST /v1/auth/token with the tickets:read scope."
        )

    params: dict = {"limit": PAGE_SIZE}
    if watermark is not None:
        params["since"] = watermark.isoformat()

    rows: list[dict] = []
    cursor: str | None = None
    with httpx.Client(base_url=base_url, timeout=30.0, headers=headers) as client:
        while True:
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor
            response = client.get("/v1/support/tickets", params=page_params)
            response.raise_for_status()
            page = response.json()
            rows.extend(page["items"])
            cursor = page.get("next_cursor")
            if not page.get("has_more") or not cursor:
                break
    return rows


def _tickets_from_api(con, cfg, watermark, base_url: str):
    rows = _fetch_pages(base_url, watermark)

    # Written to a temp file and read back with the same `read_json` the seed
    # path uses, rather than handed to DuckDB in memory. Two reasons: Bronze's
    # record hash is computed in SQL over the same VARCHAR columns either way,
    # and a capture that goes through a different code path is a capture whose
    # equivalence to the other one is an assumption rather than a fact.
    #
    # A TemporaryDirectory rather than NamedTemporaryFile(delete=False): the
    # file has to survive being closed so DuckDB can open it by name, and the
    # directory context manager is what deletes it afterwards. `delete=False`
    # with no cleanup leaks a file per run into /tmp, which on a scheduled
    # ingest is a slow disk-space leak that nothing attributes to this.
    with tempfile.TemporaryDirectory(prefix="meridian-restapi-") as directory:
        path = Path(directory) / "tickets.ndjson"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, default=str) + "\n")

        columns = ", ".join(f"'{c}': 'VARCHAR'" for c in TICKET_COLUMNS)
        yield Extract(
            select_sql=(
                f"SELECT {', '.join(TICKET_COLUMNS)} "
                f"FROM read_json('{path}', columns={{{columns}}}, "
                f"format='newline_delimited')"
            ),
            # The URL, not the temp path. `_source_file` is provenance, and a
            # /tmp name that no longer exists is not provenance.
            source_file=f"{base_url}/v1/support/tickets",
        )


def _tickets(con, cfg, watermark):
    base_url = os.environ.get("MERIDIAN_API_URL", "").rstrip("/")
    if base_url:
        yield from _tickets_from_api(con, cfg, watermark, base_url)
        return

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
