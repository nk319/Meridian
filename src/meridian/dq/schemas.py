"""Pandera schemas: the analytical contract, checked after load.

CONTRACTS.md §7 names `pandera` as one of the three sources feeding
`meta.dq_check_results`. The question worth answering is what it should assert
that is not already asserted somewhere else, because the warehouse is not short
of validation:

- `db/init` and `warehouse/ddl.sql` put CHECK constraints on every vocabulary.
  Those protect the **write** path — nothing can be inserted that violates them.
- `lake/build_silver.py` types and validates every row before it is written, and
  quarantines what fails. That protects the **load**.

Repeating either here would be theatre. So these schemas assert the thing
neither can: **distribution**. A CHECK constraint is per-row and cannot know that
72% of orders are usually delivered, that roughly an eighth of payment attempts
fail, or that the product catalogue is about 43% margin. Those are properties of
the table as a whole, they are exactly what breaks when an upstream system
changes quietly, and every row can be individually valid while the set is wrong.

The bounds below were measured against the loaded warehouse and then widened to
roughly ±20% relative. A bound guessed rather than measured either never fires or
always does, and both teach people to ignore the dashboard.

Every frame is fetched with explicit `::float8` casts. Postgres `numeric` arrives
in pandas as `Decimal` objects in an `object` column, which no numeric check can
read — the failure looks like a schema error rather than a type-mapping one.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandera.pandas as pa

# Measured, then widened. See the module docstring.
DELIVERED_SHARE = (0.60, 0.85)  # observed 0.717
CANCELLED_SHARE = (0.02, 0.12)  # observed 0.059
CAPTURED_SHARE = (0.70, 0.92)  # observed 0.809
FAILED_SHARE = (0.05, 0.22)  # observed 0.125
NEGATIVE_SENTIMENT_SHARE = (0.40, 0.72)  # observed 0.561
PURCHASE_EVENT_SHARE = (0.05, 0.16)  # observed 0.094
ACTIVE_PRODUCT_SHARE = (0.75, 0.99)  # observed 0.889
MEAN_MARGIN_PCT = (30.0, 58.0)  # observed 42.8
MEAN_ORDER_TOTAL = (90.0, 220.0)  # observed 141.17


def _share(column: str, value: str, bounds: tuple[float, float]) -> pa.Check:
    lo, hi = bounds
    return pa.Check(
        lambda df, _c=column, _v=value: lo <= (df[_c] == _v).mean() <= hi,
        name=f"share_of_{column}_{value}_between_{lo}_and_{hi}",
        error=f"share of {column}='{value}' outside [{lo}, {hi}]",
    )


ORDER_STATUS = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
CHANNEL = ["organic", "paid_search", "email", "social", "direct", "affiliate"]
DEVICE = ["desktop", "mobile", "tablet"]


@dataclass(frozen=True)
class FrameSpec:
    """A schema plus the query that feeds it."""

    entity: str
    table: str
    sql: str
    schema: pa.DataFrameSchema


SPECS: list[FrameSpec] = [
    FrameSpec(
        entity="orders",
        table="silver.orders",
        sql="""
            SELECT order_id, customer_id, status, channel, device_type,
                   total_amount::float8 AS total_amount,
                   discount_amount::float8 AS discount_amount
            FROM silver.orders
        """,
        schema=pa.DataFrameSchema(
            {
                "order_id": pa.Column(str, unique=True, nullable=False),
                "customer_id": pa.Column(str, nullable=False),
                "status": pa.Column(str, pa.Check.isin(ORDER_STATUS)),
                "channel": pa.Column(str, pa.Check.isin(CHANNEL)),
                "device_type": pa.Column(str, pa.Check.isin(DEVICE)),
                "total_amount": pa.Column(float, pa.Check.ge(0)),
                "discount_amount": pa.Column(float, pa.Check.ge(0)),
            },
            checks=[
                _share("status", "delivered", DELIVERED_SHARE),
                _share("status", "cancelled", CANCELLED_SHARE),
                pa.Check(
                    lambda df: (
                        MEAN_ORDER_TOTAL[0] <= df["total_amount"].mean() <= MEAN_ORDER_TOTAL[1]
                    ),
                    name="mean_order_total_in_range",
                    error=f"mean order total outside {MEAN_ORDER_TOTAL}",
                ),
                # Basket value is lognormal by construction, so the mean sits
                # well above the median. If that inverts, the distribution has
                # changed shape even when every individual row is still valid.
                pa.Check(
                    lambda df: df["total_amount"].mean() > df["total_amount"].median(),
                    name="order_total_right_skewed",
                    error="order totals lost their right skew",
                ),
            ],
            strict=False,
            name="silver.orders",
        ),
    ),
    FrameSpec(
        entity="order_items",
        table="silver.order_items",
        sql="""
            SELECT order_item_id, order_id, product_id, line_number, quantity,
                   unit_price::float8 AS unit_price, line_amount::float8 AS line_amount
            FROM silver.order_items
        """,
        schema=pa.DataFrameSchema(
            {
                "order_item_id": pa.Column(str, unique=True),
                "order_id": pa.Column(str, nullable=False),
                "product_id": pa.Column(str, nullable=False),
                "line_number": pa.Column(int, pa.Check.ge(1)),
                "quantity": pa.Column(int, pa.Check.in_range(1, 6)),
                "unit_price": pa.Column(float, pa.Check.ge(0)),
                "line_amount": pa.Column(float, pa.Check.ge(0)),
            },
            checks=[
                pa.Check(
                    lambda df: (
                        (df["line_amount"] - df["unit_price"] * df["quantity"]).abs().max() <= 0.01
                    ),
                    name="line_amount_reconciles",
                    error="line amount does not equal unit price times quantity",
                ),
            ],
            strict=False,
            name="silver.order_items",
        ),
    ),
    FrameSpec(
        entity="products",
        table="silver.products",
        sql="""
            SELECT product_id, sku, category, is_active,
                   unit_price::float8 AS unit_price, unit_cost::float8 AS unit_cost
            FROM silver.products
        """,
        schema=pa.DataFrameSchema(
            {
                "product_id": pa.Column(str, unique=True),
                "sku": pa.Column(str, nullable=False),
                "category": pa.Column(str, nullable=False),
                "is_active": pa.Column(bool),
                "unit_price": pa.Column(float, pa.Check.gt(0)),
                "unit_cost": pa.Column(float, pa.Check.gt(0)),
            },
            checks=[
                pa.Check(
                    lambda df: (
                        ACTIVE_PRODUCT_SHARE[0] <= df["is_active"].mean() <= ACTIVE_PRODUCT_SHARE[1]
                    ),
                    name="active_product_share_in_range",
                    error=f"active product share outside {ACTIVE_PRODUCT_SHARE}",
                ),
                # Margin is the number a pricing bug moves first, and no row-level
                # constraint can see it: every price and cost can be individually
                # plausible while the spread between them collapses.
                pa.Check(
                    lambda df: (
                        MEAN_MARGIN_PCT[0]
                        <= (100 * (df["unit_price"] - df["unit_cost"]) / df["unit_price"]).mean()
                        <= MEAN_MARGIN_PCT[1]
                    ),
                    name="mean_margin_pct_in_range",
                    error=f"mean gross margin outside {MEAN_MARGIN_PCT}%",
                ),
                pa.Check(
                    lambda df: (df["unit_cost"] < df["unit_price"]).all(),
                    name="cost_below_price",
                    error="a product is priced below cost",
                ),
            ],
            strict=False,
            name="silver.products",
        ),
    ),
    FrameSpec(
        entity="payments",
        table="silver.payments",
        sql="""
            SELECT payment_id, order_id, status, payment_method, attempt_number,
                   amount::float8 AS amount
            FROM silver.payments
        """,
        schema=pa.DataFrameSchema(
            {
                "payment_id": pa.Column(str, unique=True),
                "order_id": pa.Column(str, nullable=False),
                "status": pa.Column(
                    str,
                    pa.Check.isin(["authorized", "captured", "failed", "refunded", "chargeback"]),
                ),
                "payment_method": pa.Column(
                    str, pa.Check.isin(["card", "paypal", "bank_transfer", "gift_card"])
                ),
                "attempt_number": pa.Column(int, pa.Check.in_range(1, 2)),
                "amount": pa.Column(float, pa.Check.ge(0)),
            },
            checks=[
                _share("status", "captured", CAPTURED_SHARE),
                # Authorisation failure rate is the payment health mart's headline
                # number. A sudden move in it is a genuine incident, and it is
                # invisible to every per-row rule.
                _share("status", "failed", FAILED_SHARE),
            ],
            strict=False,
            name="silver.payments",
        ),
    ),
    FrameSpec(
        entity="support_tickets",
        table="silver.support_tickets",
        sql="""
            SELECT ticket_id, customer_id, status, channel, intent, priority, sentiment
            FROM silver.support_tickets
        """,
        schema=pa.DataFrameSchema(
            {
                "ticket_id": pa.Column(str, unique=True),
                "customer_id": pa.Column(str, nullable=False),
                "status": pa.Column(str, pa.Check.isin(["open", "pending", "resolved"])),
                "channel": pa.Column(str, pa.Check.isin(["email", "chat", "phone", "web_form"])),
                "intent": pa.Column(
                    str,
                    pa.Check.isin(
                        [
                            "shipping_delay",
                            "refund_request",
                            "product_defect",
                            "billing_question",
                            "account_access",
                            "return_process",
                            "general_inquiry",
                        ]
                    ),
                ),
                "priority": pa.Column(str, pa.Check.isin(["P1", "P2", "P3", "P4"])),
                "sentiment": pa.Column(str, pa.Check.isin(["positive", "neutral", "negative"])),
            },
            checks=[
                _share("sentiment", "negative", NEGATIVE_SENTIMENT_SHARE),
                # Every intent must still be represented. If one disappears the
                # RAG golden set silently loses a question's relevant set, and
                # recall drops for a reason that has nothing to do with retrieval.
                pa.Check(
                    lambda df: df["intent"].nunique() == 7,
                    name="all_seven_intents_present",
                    error="an intent vanished from the corpus",
                ),
            ],
            strict=False,
            name="silver.support_tickets",
        ),
    ),
    FrameSpec(
        entity="web_events",
        table="silver.web_events",
        sql="""
            SELECT event_id, session_id, event_type, channel, device_type
            FROM silver.web_events
        """,
        schema=pa.DataFrameSchema(
            {
                "event_id": pa.Column(str, unique=True),
                "session_id": pa.Column(str, nullable=False),
                "event_type": pa.Column(
                    str,
                    pa.Check.isin(
                        [
                            "page_view",
                            "product_view",
                            "add_to_cart",
                            "begin_checkout",
                            "purchase",
                            "search",
                        ]
                    ),
                ),
                "channel": pa.Column(str, pa.Check.isin(CHANNEL)),
                "device_type": pa.Column(str, pa.Check.isin(DEVICE)),
            },
            checks=[
                # The funnel's conversion rate. If this drifts the web funnel mart
                # is describing a different business.
                _share("event_type", "purchase", PURCHASE_EVENT_SHARE),
                pa.Check(
                    lambda df: (
                        (df["event_type"] == "page_view").sum()
                        >= (df["event_type"] == "purchase").sum()
                    ),
                    name="funnel_is_a_funnel",
                    error="more purchases than page views; the funnel inverted",
                ),
            ],
            strict=False,
            name="silver.web_events",
        ),
    ),
    FrameSpec(
        entity="customers",
        table="silver.customers",
        sql="""
            SELECT customer_id, city, country, loyalty_tier, segment, is_deleted
            FROM silver.customers
        """,
        schema=pa.DataFrameSchema(
            {
                "customer_id": pa.Column(str, unique=True),
                "city": pa.Column(str, nullable=False),
                "country": pa.Column(str, nullable=False),
                "loyalty_tier": pa.Column(
                    str, pa.Check.isin(["bronze", "silver", "gold", "platinum"])
                ),
                "segment": pa.Column(
                    str, pa.Check.isin(["new", "active", "at_risk", "churned", "vip"])
                ),
                "is_deleted": pa.Column(bool),
            },
            checks=[
                pa.Check(
                    lambda df: df["loyalty_tier"].nunique() == 4,
                    name="all_four_tiers_present",
                    error="a loyalty tier vanished; SCD2 transitions would have nothing to move between",
                ),
                pa.Check(
                    lambda df: df["is_deleted"].mean() < 0.05,
                    name="hard_deletes_are_rare",
                    error="an implausible share of customers are flagged deleted",
                ),
            ],
            strict=False,
            name="silver.customers",
        ),
    ),
]
