"""Capture the CSV/JSON file drop into Bronze.

    python -m meridian.ingest.files --mode full
    python -m meridian.ingest.files --mode incremental --entity web_events

Everything lands as VARCHAR. This feed is where the generator injects malformed
dates, invalid enums, blanked required fields and duplicate rows, and typing on
the way in would reject those rows at the door — leaving no trace, and no way to
tell a rejected row from one that was never sent. Captured as text, each one
reaches Silver and is quarantined with a reason.

The incremental filter keeps unparseable timestamps rather than dropping them,
and captures each of them exactly once. See `base.incremental_where`: getting
the first half right and the second half wrong re-ingests every malformed row
on every run, which is how that predicate was written the first time.
"""

from __future__ import annotations

from .base import EntitySpec, Extract, exit_with, incremental_where, run_source

SOURCE = "files"

PRODUCT_COLUMNS = (
    "product_id",
    "sku",
    "product_name",
    "category",
    "subcategory",
    "unit_price",
    "unit_cost",
    "is_active",
)
WEB_EVENT_COLUMNS = (
    "event_id",
    "session_id",
    "customer_id",
    "event_ts",
    "event_type",
    "product_id",
    "order_id",
    "channel",
    "device_type",
)


def _read_csv(path: str) -> str:
    return f"read_csv('{path}', header=true, all_varchar=true)"


def _products(con, cfg, watermark):
    path = cfg.seeds_dir / "files" / "products.csv"
    yield Extract(
        select_sql=f"SELECT {', '.join(PRODUCT_COLUMNS)} FROM {_read_csv(path)}",
        source_file="files:products.csv",
    )


def _web_events(con, cfg, watermark):
    path = cfg.seeds_dir / "files" / "web_events.csv"
    where = incremental_where(
        con,
        cfg,
        source=SOURCE,
        entity="web_events",
        ts_column="event_ts",
        watermark=watermark,
        business_columns=WEB_EVENT_COLUMNS,
    )
    yield Extract(
        select_sql=f"SELECT {', '.join(WEB_EVENT_COLUMNS)} FROM {_read_csv(path)} {where}",
        source_file="files:web_events.csv",
    )


SPECS = [
    # products has no event timestamp at all, so it is full-refresh only and
    # says so rather than accepting --mode incremental and quietly re-reading
    # the whole file every time.
    EntitySpec("products", PRODUCT_COLUMNS, None, _products),
    EntitySpec("web_events", WEB_EVENT_COLUMNS, "event_ts", _web_events),
]


def main() -> int:
    return run_source(SOURCE, SPECS)


if __name__ == "__main__":
    exit_with(main)
