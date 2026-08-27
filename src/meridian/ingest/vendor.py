"""Capture the PayFlow vendor feed into Bronze, following its cursor.

    python -m meridian.ingest.vendor --mode incremental

The vendor serves payments in pages of 500, each carrying `has_more` and a
`next_cursor`. This walks that chain rather than globbing the directory, because
globbing is not the thing that has to work in production: a real vendor API
hands you one page and a cursor, and the failure modes worth exercising —
a cursor that points at a page that is not there, a `has_more` that never goes
false — only exist if the client actually follows it.

Each page becomes its own Bronze part file. That is what `part-{run_id}-{seq}`
in the §2 layout is for, and it means `_batch_seq` stays monotonic across the
whole run rather than restarting per file: the ordering of a run's rows survives
being split across 33 objects.
"""

from __future__ import annotations

import json
from pathlib import Path

from .base import EntitySpec, Extract, exit_with, incremental_where, run_source

SOURCE = "vendor"

PAYMENT_COLUMNS = (
    "payment_id",
    "order_id",
    "attempt_number",
    "payment_method",
    "status",
    "amount",
    "processed_ts",
    "failure_reason",
)

# The vendor's cursors are opaque to a client, so resolving one is the
# transport's job. Here that means a filename; in Phase 6 it means a query
# parameter. Nothing else in this module changes when it does.
MAX_PAGES = 10_000


def _page_path(directory: Path, page_no: int) -> Path:
    return directory / f"payments_page_{page_no:03d}.json"


def _cursor_to_page(cursor: str) -> int:
    try:
        return int(cursor.rsplit("_", 1)[1])
    except (IndexError, ValueError) as exc:
        raise ValueError(f"vendor returned an unparseable cursor {cursor!r}") from exc


def _payments(con, cfg, watermark):
    directory = cfg.seeds_dir / "vendor"
    columns = ", ".join(PAYMENT_COLUMNS)
    where = incremental_where(
        con,
        cfg,
        source=SOURCE,
        entity="payments",
        ts_column="processed_ts",
        watermark=watermark,
        business_columns=PAYMENT_COLUMNS,
    )

    page_no = 0
    seen: set[int] = set()
    for _ in range(MAX_PAGES):
        path = _page_path(directory, page_no)
        if not path.is_file():
            raise FileNotFoundError(
                f"the vendor cursor points at page {page_no}, which does not exist "
                f"({path}). The feed is truncated or the cursor is wrong."
            )
        # A vendor whose next_cursor loops back would otherwise spin forever,
        # re-ingesting the same page until the disk fills.
        if page_no in seen:
            raise RuntimeError(f"vendor pagination revisited page {page_no}; cursor loop")
        seen.add(page_no)

        payload = json.loads(path.read_text(encoding="utf-8"))
        casts = ", ".join(f"CAST(r.{c} AS VARCHAR) AS {c}" for c in PAYMENT_COLUMNS)
        yield Extract(
            select_sql=(
                f"SELECT {columns} FROM ("
                f"  SELECT {casts} FROM ("
                f"    SELECT unnest(data) AS r FROM read_json('{path}')"
                f"  )"
                f") {where}"
            ),
            source_file=f"vendor:/v1/payments?cursor=page_{page_no}",
        )

        if not payload.get("has_more") or payload.get("next_cursor") is None:
            return
        page_no = _cursor_to_page(payload["next_cursor"])
    raise RuntimeError(f"vendor pagination exceeded {MAX_PAGES} pages; has_more never cleared")


SPECS = [EntitySpec("payments", PAYMENT_COLUMNS, "processed_ts", _payments)]


def main() -> int:
    return run_source(SOURCE, SPECS)


if __name__ == "__main__":
    exit_with(main)
