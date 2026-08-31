"""Capture the OLTP source database into Bronze.

    python -m meridian.ingest.oltp --mode full
    python -m meridian.ingest.oltp --mode incremental --entity orders

Reads through DuckDB's Postgres extension with a READ_ONLY attach: this is the
live source system, and an ingestion job holding a writable handle to it is one
typo away from being the thing that corrupts the data it exists to copy.

Values arrive already typed, and they are kept that way. Bronze is "what the
source actually sent", and from a relational database that is a typed row — a
blanket cast to text would throw away the source's own type information and
force Silver to re-parse something that was never ambiguous. The file and API
feeds, which really are text on the wire and really do carry malformed values,
land as VARCHAR for exactly the same reason.

`support_tickets` lives in this database and is deliberately NOT ingested here.
CONTRACTS.md §5 assigns it to the `restapi` source, and one entity has one
owner: an earlier design had a single entity arriving over four paths at once,
which double-counted it in Bronze and made reconciliation fail permanently.
base.OWNERSHIP enforces that; this comment explains why the obvious-looking
addition is wrong.
"""

from __future__ import annotations

import datetime as dt

from ..lake.duck import attach_oltp
from .base import EntitySpec, Extract, exit_with, run_source

SOURCE = "oltp"


def _since(column: str, watermark: dt.datetime | None) -> str:
    if watermark is None:
        return ""
    return f"WHERE {column} > TIMESTAMPTZ '{watermark.isoformat()}'"


def _table(entity: str, columns: tuple[str, ...], watermark_column: str):
    def build(con, cfg, watermark):
        yield Extract(
            select_sql=(
                f"SELECT {', '.join(columns)} FROM src.public.{entity} "
                f"{_since(watermark_column, watermark)}"
            ),
            source_file=f"oltp:public.{entity}",
        )

    return build


CUSTOMER_COLUMNS = (
    "customer_id",
    "city",
    "country",
    "signup_date",
    "loyalty_tier",
    "segment",
    "is_deleted",
    "updated_at",
)
ORDER_COLUMNS = (
    "order_id",
    "customer_id",
    "order_ts",
    "order_date",
    "status",
    "channel",
    "device_type",
    "gross_amount",
    "discount_amount",
    "shipping_amount",
    "tax_amount",
    "total_amount",
)
# order_ts is carried from the parent order. The source table has no timestamp
# of its own, so without it this entity could only ever be full-refreshed — and
# a full refresh of the largest child table on every run is the thing
# incremental ingestion exists to avoid. Silver drops it again; it is capture
# machinery, not a business column.
ORDER_ITEM_COLUMNS = (
    "order_item_id",
    "order_id",
    "line_number",
    "product_id",
    "quantity",
    "unit_price",
    "line_amount",
    "order_ts",
)
CHANGE_LOG_COLUMNS = ("customer_id", "changed_at", "field", "old_value", "new_value")


def _order_items(con, cfg, watermark):
    where = ""
    if watermark is not None:
        where = f"WHERE o.order_ts > TIMESTAMPTZ '{watermark.isoformat()}'"
    yield Extract(
        select_sql=f"""
            SELECT i.order_item_id, i.order_id, i.line_number, i.product_id,
                   i.quantity, i.unit_price, i.line_amount, o.order_ts
            FROM src.public.order_items i
            JOIN src.public.orders o ON o.order_id = i.order_id
            {where}
        """,
        source_file="oltp:public.order_items",
    )


SPECS = [
    EntitySpec(
        "customers",
        CUSTOMER_COLUMNS,
        "updated_at",
        _table("customers", CUSTOMER_COLUMNS, "updated_at"),
    ),
    EntitySpec("orders", ORDER_COLUMNS, "order_ts", _table("orders", ORDER_COLUMNS, "order_ts")),
    EntitySpec("order_items", ORDER_ITEM_COLUMNS, "order_ts", _order_items),
    EntitySpec(
        "customer_change_log",
        CHANGE_LOG_COLUMNS,
        "changed_at",
        _table("customer_change_log", CHANGE_LOG_COLUMNS, "changed_at"),
        order_by="customer_id, changed_at, field",
    ),
]


def main() -> int:
    return run_source(SOURCE, SPECS, setup=lambda con, cfg: attach_oltp(con, cfg))


if __name__ == "__main__":
    exit_with(main)
