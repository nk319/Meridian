"""The declared suites.

Every threshold in here was measured against the real warehouse before it was
written down. A tolerance chosen by guessing is a check that either never fires
or always does, and both are worse than no check at all — the first is decoration
and the second trains people to ignore the dashboard.
"""

from __future__ import annotations

from .checks import SqlCheck, invariant, not_empty, referential, unique_key

SILVER_TABLES = (
    "silver.customers",
    "silver.orders",
    "silver.order_items",
    "silver.customer_change_log",
    "silver.products",
    "silver.web_events",
    "silver.support_tickets",
    "silver.payments",
)


# ---------------------------------------------------------------------------
# referential integrity
# ---------------------------------------------------------------------------

REFERENTIAL: list[SqlCheck] = [
    referential("silver.orders", "customer_id", "silver.customers", "customer_id"),
    referential("silver.order_items", "order_id", "silver.orders", "order_id"),
    referential("silver.payments", "order_id", "silver.orders", "order_id"),
    referential("silver.support_tickets", "order_id", "silver.orders", "order_id"),
    referential("silver.support_tickets", "customer_id", "silver.customers", "customer_id"),
    referential("silver.web_events", "customer_id", "silver.customers", "customer_id"),
    # The one that is expected to fail, and the most interesting check here.
    #
    # Quarantine fans out. Three product rows were rejected for bad data — 1.36%
    # of the catalogue — and that orphaned 380 order lines, 1.46% of the table.
    # The lines themselves are perfectly valid; their product is simply not in
    # Silver, so any join to dim_product silently drops them.
    #
    # So this is a WARN rather than a BLOCK: nothing is wrong with the pipeline
    # and stopping it would be wrong. The tolerance is set at roughly twice the
    # observed rate, which is what makes it a regression detector — it passes at
    # today's quarantine level and fires if product rejection meaningfully
    # worsens.
    referential(
        "silver.order_items",
        "product_id",
        "silver.products",
        "product_id",
        severity="WARN",
        max_failure_pct=3.0,
        message=(
            "order lines whose product was quarantined; they are valid rows that "
            "will vanish from any product join"
        ),
    ),
]


# ---------------------------------------------------------------------------
# business invariants
# ---------------------------------------------------------------------------

BUSINESS: list[SqlCheck] = [
    invariant(
        "order_total_reconciles",
        "silver.orders",
        "abs(total_amount - (gross_amount - discount_amount + shipping_amount + tax_amount)) <= 0.01",
        "an order total must equal gross - discount + shipping + tax",
    ),
    invariant(
        "line_amount_reconciles",
        "silver.order_items",
        "abs(line_amount - unit_price * quantity) <= 0.01",
        "a line amount must equal unit price times quantity",
    ),
    invariant(
        "payment_matches_order_total",
        "silver.payments p",
        "EXISTS (SELECT 1 FROM silver.orders o WHERE o.order_id = p.order_id "
        "        AND abs(p.amount - o.total_amount) <= 0.01)",
        "a payment attempt must be for the order's total",
    ),
    invariant(
        "failed_payment_has_reason",
        "silver.payments",
        "failure_reason IS NOT NULL AND failure_reason <> ''",
        "a failed payment must record why",
        scope="status = 'failed'",
    ),
    invariant(
        "resolved_ticket_has_timestamp",
        "silver.support_tickets",
        "resolved_ts IS NOT NULL",
        "a resolved ticket must have a resolution time",
        scope="status = 'resolved'",
    ),
    invariant(
        "order_has_line_items",
        "silver.orders o",
        "EXISTS (SELECT 1 FROM silver.order_items i WHERE i.order_id = o.order_id)",
        "an order with no lines has nothing to have been sold",
    ),
    invariant(
        "purchase_event_has_order",
        "silver.web_events w",
        "EXISTS (SELECT 1 FROM silver.orders o WHERE o.order_id = w.order_id)",
        "a purchase event must reference a real order, or the funnel's last step "
        "is an independent number that never reconciles with revenue",
        scope="event_type = 'purchase' AND order_id IS NOT NULL",
    ),
    invariant(
        "scd2_demo_customer_still_changes",
        "silver.customer_change_log",
        "TRUE",
        "the SCD2 demo customer must have loyalty tier transitions, or Phase 4's "
        "dimension tests become tautologies over an empty result set",
        scope="customer_id = 'C000042' AND field = 'loyalty_tier'",
    ),
]


# ---------------------------------------------------------------------------
# volume and shape
# ---------------------------------------------------------------------------

VOLUME: list[SqlCheck] = [not_empty(t) for t in SILVER_TABLES] + [
    unique_key("silver.customers", ("customer_id",)),
    unique_key("silver.orders", ("order_id",)),
    unique_key("silver.order_items", ("order_id", "line_number")),
    unique_key("silver.products", ("product_id",)),
    unique_key("silver.support_tickets", ("ticket_id",)),
    unique_key("silver.payments", ("payment_id",)),
]


# ---------------------------------------------------------------------------
# freshness
# ---------------------------------------------------------------------------


def freshness(max_age_hours: int = 24) -> list[SqlCheck]:
    """Every pipeline step has succeeded recently.

    Freshness is asserted about the *pipeline*, not about the data. The
    generator writes to a fixed anchor date so the newest order is always the
    same age, and a check on `max(order_ts)` would fail purely because time
    passed — a red dashboard that means nothing, which is how people learn to
    ignore red dashboards.

    "The last successful run of each step completed within N hours" is the thing
    that is actually true or false about a running platform.
    """
    return [
        SqlCheck(
            name="pipeline_step_freshness",
            target_table="meta.pipeline_run_log",
            severity="WARN",
            message=f"every pipeline step must have succeeded within {max_age_hours}h",
            count_sql=f"""
                SELECT count(*), count(*) FILTER (
                    WHERE completed_at < now() - interval '{max_age_hours} hours')
                FROM (
                    SELECT DISTINCT ON (step) step, completed_at
                    FROM meta.pipeline_run_log
                    WHERE status = 'SUCCESS' AND completed_at IS NOT NULL
                    ORDER BY step, completed_at DESC
                ) latest
            """,
            repro_sql="""
                SELECT DISTINCT ON (step) step, completed_at,
                       now() - completed_at AS age
                FROM meta.pipeline_run_log
                WHERE status = 'SUCCESS' AND completed_at IS NOT NULL
                ORDER BY step, completed_at DESC
            """,
        ),
        SqlCheck(
            name="no_stuck_pipeline_runs",
            target_table="meta.pipeline_run_log",
            severity="WARN",
            message="a run still marked RUNNING after an hour has crashed without saying so",
            count_sql="""
                SELECT count(*), count(*) FILTER (
                    WHERE status = 'RUNNING' AND started_at < now() - interval '1 hour')
                FROM meta.pipeline_run_log
            """,
            repro_sql="""
                SELECT * FROM meta.pipeline_run_log
                WHERE status = 'RUNNING' AND started_at < now() - interval '1 hour'
            """,
        ),
    ]


# ---------------------------------------------------------------------------
# assembly
# ---------------------------------------------------------------------------


def build(max_age_hours: int = 24) -> dict[str, list[SqlCheck]]:
    suites: dict[str, list[SqlCheck]] = {
        "referential": REFERENTIAL,
        "business": BUSINESS,
        "volume": VOLUME,
        "freshness": freshness(max_age_hours),
    }
    suites["all"] = [c for name, checks in suites.items() if name != "all" for c in checks]
    return suites


SUITE_NAMES = ("all", "referential", "business", "volume", "freshness", "schema")
