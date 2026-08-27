"""What each Silver entity is allowed to contain.

Declarative on purpose. The alternative — a hand-written transform per entity —
means eight places where a cast, a null check and an enum list can quietly
disagree with CONTRACTS.md §9, and no way to see at a glance which rules an
entity is actually enforcing.

Every vocabulary here is a copy of the frozen §9 list. That duplication is
deliberate and narrow: `meridian.seed.config` holds the same values, but the
seed package is stdlib-only and importing it from the lake would tie the
pipeline's validation to the generator's presence. `tests/test_silver_spec.py`
asserts the two agree, which is the check that keeps a copy honest.
"""

from __future__ import annotations

from dataclasses import dataclass, field

ORDER_STATUS = ("pending", "confirmed", "shipped", "delivered", "cancelled", "returned")
PAYMENT_STATUS = ("authorized", "captured", "failed", "refunded", "chargeback")
PAYMENT_METHOD = ("card", "paypal", "bank_transfer", "gift_card")
LOYALTY_TIER = ("bronze", "silver", "gold", "platinum")
CUSTOMER_SEGMENT = ("new", "active", "at_risk", "churned", "vip")
CHANNEL = ("organic", "paid_search", "email", "social", "direct", "affiliate")
DEVICE_TYPE = ("desktop", "mobile", "tablet")
EVENT_TYPE = ("page_view", "product_view", "add_to_cart", "begin_checkout", "purchase", "search")
TICKET_INTENT = (
    "shipping_delay",
    "refund_request",
    "product_defect",
    "billing_question",
    "account_access",
    "return_process",
    "general_inquiry",
)
TICKET_PRIORITY = ("P1", "P2", "P3", "P4")
SENTIMENT = ("positive", "neutral", "negative")

# Not in §9 — the source system's ticket lifecycle, recorded in CONTRACTS.md's
# deviations table until Phase 4 promotes it.
TICKET_STATUS = ("open", "pending", "resolved")
TICKET_CHANNEL = ("email", "chat", "phone", "web_form")


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    nullable: bool = False
    enum: tuple[str, ...] | None = None
    # A floor, not a sign flag: quantity and line_number must be >= 1, while an
    # amount may legitimately be 0. Writing the number down beats two booleans.
    minimum: float | None = None


@dataclass(frozen=True)
class SilverSpec:
    entity: str
    source: str
    # Natural key. Bronze is append-only, so an entity ingested twice holds two
    # versions of a changed row; this is what collapses them to one.
    key: tuple[str, ...]
    columns: tuple[Column, ...]
    # Which version wins. The business timestamp where one exists, because
    # _ingested_at only says when we heard about a row, not when it changed.
    recency: str = "_ingested_at"
    # Columns Bronze carries for capture machinery that Silver has no use for.
    drop: tuple[str, ...] = field(default_factory=tuple)

    @property
    def business_columns(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)


SPECS: dict[str, SilverSpec] = {
    "customers": SilverSpec(
        entity="customers",
        source="oltp",
        key=("customer_id",),
        recency="updated_at",
        columns=(
            Column("customer_id", "VARCHAR"),
            Column("city", "VARCHAR"),
            Column("country", "VARCHAR"),
            Column("signup_date", "DATE"),
            Column("loyalty_tier", "VARCHAR", enum=LOYALTY_TIER),
            Column("segment", "VARCHAR", enum=CUSTOMER_SEGMENT),
            Column("is_deleted", "BOOLEAN"),
            Column("updated_at", "TIMESTAMPTZ"),
        ),
    ),
    "orders": SilverSpec(
        entity="orders",
        source="oltp",
        key=("order_id",),
        recency="order_ts",
        columns=(
            Column("order_id", "VARCHAR"),
            Column("customer_id", "VARCHAR"),
            Column("order_ts", "TIMESTAMPTZ"),
            Column("order_date", "DATE"),
            Column("status", "VARCHAR", enum=ORDER_STATUS),
            Column("channel", "VARCHAR", enum=CHANNEL),
            Column("device_type", "VARCHAR", enum=DEVICE_TYPE),
            Column("gross_amount", "DECIMAL(12,2)", minimum=0),
            Column("discount_amount", "DECIMAL(12,2)", minimum=0),
            Column("shipping_amount", "DECIMAL(12,2)", minimum=0),
            Column("tax_amount", "DECIMAL(12,2)", minimum=0),
            Column("total_amount", "DECIMAL(12,2)", minimum=0),
        ),
    ),
    "order_items": SilverSpec(
        entity="order_items",
        source="oltp",
        key=("order_item_id",),
        # order_ts belongs to the parent order and exists in Bronze only so this
        # entity can be captured incrementally at all. It is not a fact about a
        # line item, so it stops here.
        recency="order_ts",
        drop=("order_ts",),
        columns=(
            Column("order_item_id", "VARCHAR"),
            Column("order_id", "VARCHAR"),
            Column("line_number", "INTEGER", minimum=1),
            Column("product_id", "VARCHAR"),
            Column("quantity", "INTEGER", minimum=1),
            Column("unit_price", "DECIMAL(12,2)", minimum=0),
            Column("line_amount", "DECIMAL(12,2)", minimum=0),
        ),
    ),
    "customer_change_log": SilverSpec(
        entity="customer_change_log",
        source="oltp",
        key=("customer_id", "changed_at", "field"),
        recency="changed_at",
        columns=(
            Column("customer_id", "VARCHAR"),
            Column("changed_at", "TIMESTAMPTZ"),
            Column("field", "VARCHAR"),
            Column("old_value", "VARCHAR", nullable=True),
            Column("new_value", "VARCHAR", nullable=True),
        ),
    ),
    "products": SilverSpec(
        entity="products",
        source="files",
        key=("product_id",),
        columns=(
            Column("product_id", "VARCHAR"),
            Column("sku", "VARCHAR"),
            Column("product_name", "VARCHAR"),
            Column("category", "VARCHAR"),
            Column("subcategory", "VARCHAR"),
            Column("unit_price", "DECIMAL(12,2)", minimum=0),
            Column("unit_cost", "DECIMAL(12,2)", minimum=0),
            Column("is_active", "BOOLEAN"),
        ),
    ),
    "web_events": SilverSpec(
        entity="web_events",
        source="files",
        key=("event_id",),
        recency="event_ts",
        columns=(
            Column("event_id", "VARCHAR"),
            Column("session_id", "VARCHAR"),
            # Anonymous browsing is real; a null customer here is data, not a defect.
            Column("customer_id", "VARCHAR", nullable=True),
            Column("event_ts", "TIMESTAMPTZ"),
            Column("event_type", "VARCHAR", enum=EVENT_TYPE),
            Column("product_id", "VARCHAR", nullable=True),
            Column("order_id", "VARCHAR", nullable=True),
            Column("channel", "VARCHAR", enum=CHANNEL),
            Column("device_type", "VARCHAR", enum=DEVICE_TYPE),
        ),
    ),
    "support_tickets": SilverSpec(
        entity="support_tickets",
        source="restapi",
        key=("ticket_id",),
        recency="created_ts",
        columns=(
            Column("ticket_id", "VARCHAR"),
            Column("customer_id", "VARCHAR"),
            Column("order_id", "VARCHAR"),
            Column("created_ts", "TIMESTAMPTZ"),
            # Empty until the ticket is resolved. Null is the honest
            # representation; the epoch would invent a resolution.
            Column("resolved_ts", "TIMESTAMPTZ", nullable=True),
            Column("status", "VARCHAR", enum=TICKET_STATUS),
            Column("channel", "VARCHAR", enum=TICKET_CHANNEL),
            Column("subject", "VARCHAR"),
            Column("body", "VARCHAR"),
            Column("intent", "VARCHAR", enum=TICKET_INTENT),
            Column("priority", "VARCHAR", enum=TICKET_PRIORITY),
            Column("sentiment", "VARCHAR", enum=SENTIMENT),
        ),
    ),
    "payments": SilverSpec(
        entity="payments",
        source="vendor",
        key=("payment_id",),
        recency="processed_ts",
        columns=(
            Column("payment_id", "VARCHAR"),
            Column("order_id", "VARCHAR"),
            Column("attempt_number", "INTEGER", minimum=1),
            Column("payment_method", "VARCHAR", enum=PAYMENT_METHOD),
            Column("status", "VARCHAR", enum=PAYMENT_STATUS),
            Column("amount", "DECIMAL(12,2)", minimum=0),
            Column("processed_ts", "TIMESTAMPTZ"),
            # Only populated on a failed attempt.
            Column("failure_reason", "VARCHAR", nullable=True),
        ),
    ),
}
