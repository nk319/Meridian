"""Frozen vocabularies and generation constants.

Enum values here are the ones in docs/CONTRACTS.md §9. The Pydantic models, dbt
tests and dashboard filters all read from this module, so the vocabulary exists
in exactly one place.
"""

from __future__ import annotations

import datetime as dt

# --- determinism -----------------------------------------------------------
# The anchor is fixed rather than derived from "today" so that two runs a week
# apart produce byte-identical output. Override with --anchor-date when you want
# the demo data to end near the current date.
DEFAULT_SEED = 20260827
DEFAULT_ANCHOR = dt.date(2026, 8, 1)
DEFAULT_MONTHS = 24

# --- volumes ---------------------------------------------------------------
# Sized so a full generate + load cycle runs in well under a minute on a laptop
# while still producing enough history for cohort retention and RFM quintiles to
# be meaningful. 90 days of data makes the cohort heatmap a 3-row triangle and
# collapses NTILE(5) into five identical buckets.
DEFAULT_CUSTOMERS = 3000
DEFAULT_PRODUCTS = 220
AVG_ORDERS_PER_ACTIVE_CUSTOMER = 6.5
WEB_EVENTS_PER_ORDER = 11.0
TICKETS_PER_100_ORDERS = 9.0

# --- vocabularies (CONTRACTS.md §9) ---------------------------------------
ORDER_STATUS = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
PAYMENT_STATUS = ["authorized", "captured", "failed", "refunded", "chargeback"]
PAYMENT_METHOD = ["card", "paypal", "bank_transfer", "gift_card"]
LOYALTY_TIER = ["bronze", "silver", "gold", "platinum"]
CUSTOMER_SEGMENT = ["new", "active", "at_risk", "churned", "vip"]
CHANNEL = ["organic", "paid_search", "email", "social", "direct", "affiliate"]
DEVICE_TYPE = ["desktop", "mobile", "tablet"]
EVENT_TYPE = [
    "page_view",
    "product_view",
    "add_to_cart",
    "begin_checkout",
    "purchase",
    "search",
]
TICKET_INTENT = [
    "shipping_delay",
    "refund_request",
    "product_defect",
    "billing_question",
    "account_access",
    "return_process",
    "general_inquiry",
]
TICKET_PRIORITY = ["P1", "P2", "P3", "P4"]
SENTIMENT = ["positive", "neutral", "negative"]

# --- weighted distributions ------------------------------------------------
# Terminal order states. Most orders complete; the tail is what makes the
# payment-health and returns marts non-trivial.
ORDER_STATUS_WEIGHTS = {
    "delivered": 0.72,
    "shipped": 0.09,
    "confirmed": 0.06,
    "pending": 0.03,
    "cancelled": 0.06,
    "returned": 0.04,
}
PAYMENT_METHOD_WEIGHTS = {"card": 0.68, "paypal": 0.19, "bank_transfer": 0.07, "gift_card": 0.06}
CHANNEL_WEIGHTS = {
    "organic": 0.28,
    "paid_search": 0.22,
    "email": 0.16,
    "social": 0.15,
    "direct": 0.12,
    "affiliate": 0.07,
}
DEVICE_WEIGHTS = {"mobile": 0.54, "desktop": 0.38, "tablet": 0.08}
TIER_WEIGHTS = {"bronze": 0.52, "silver": 0.28, "gold": 0.15, "platinum": 0.05}

# Basket value is lognormal: a long right tail, no negative values, which is how
# real order values behave. mu/sigma are in log space.
BASKET_LOG_MU = 4.05
BASKET_LOG_SIGMA = 0.62

# Order frequency follows a Pareto: a small share of customers place most orders.
ORDER_FREQ_PARETO_ALPHA = 1.35
# Compensates for int() truncation of the Pareto draw and for the seasonality
# rejection step, both of which shed orders. Tuned so the realised average lands
# near AVG_ORDERS_PER_ACTIVE_CUSTOMER above.
ORDER_FREQ_SCALE = 7.0

# Seasonality multipliers by month (1-indexed). November/December carry the peak.
MONTH_SEASONALITY = {
    1: 0.82, 2: 0.78, 3: 0.90, 4: 0.94, 5: 1.00, 6: 0.97,
    7: 0.93, 8: 0.95, 9: 1.04, 10: 1.12, 11: 1.46, 12: 1.58,
}

# --- SCD2 demonstration ----------------------------------------------------
# A snapshot over data that never changes yields one version per customer and
# turns its own tests into tautologies. These two customers guarantee the
# dimension is genuinely exercised. Offsets are days back from the anchor.
SCD2_DEMO_CUSTOMER_ID = "C000042"
SCD2_DEMO_TRANSITIONS = [
    (540, "bronze", "silver"),
    (330, "silver", "gold"),
    (120, "gold", "platinum"),
]
HARD_DELETE_CUSTOMER_ID = "C000117"
HARD_DELETE_DAYS_AGO = 200

# --- categories ------------------------------------------------------------
CATEGORIES = {
    "Audio": ["Headphones", "Speakers", "Earbuds", "Turntables"],
    "Computing": ["Laptops", "Monitors", "Keyboards", "Storage"],
    "Home": ["Lighting", "Kitchen", "Bedding", "Storage"],
    "Outdoor": ["Camping", "Cycling", "Footwear", "Packs"],
    "Wellness": ["Fitness", "Recovery", "Supplements", "Sleep"],
}
