"""Products, customers, orders, order items, payments and web events.

Everything is derived from a single seeded Random, so a given (seed, anchor,
volume) triple always produces byte-identical output.

Referential integrity is guaranteed by construction rather than checked after the
fact: orders are only ever built from a customer that exists and products drawn
from the catalogue, payments are built from an order, and web events are built
from a session that belongs to a customer.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import random

from . import config as C


def _weighted(rng: random.Random, weights: dict[str, float]) -> str:
    return rng.choices(list(weights), weights=list(weights.values()), k=1)[0]


def _round2(x: float) -> float:
    return float(f"{x:.2f}")


# --------------------------------------------------------------------------
# products
# --------------------------------------------------------------------------

def make_products(rng: random.Random, n: int) -> list[dict]:
    products = []
    pid = 0
    categories = list(C.CATEGORIES.items())

    while pid < n:
        category, subcats = categories[pid % len(categories)]
        subcategory = rng.choice(subcats)
        pid += 1

        # Price is lognormal per category so categories have distinct price bands
        # rather than one global distribution sliced arbitrarily.
        #
        # sha1 rather than hash(): CPython randomises string hashing per process
        # unless PYTHONHASHSEED is pinned, which would make the generator
        # non-deterministic across runs and every downstream row-count assertion
        # flaky. Caught by test_generator_is_deterministic.
        band = 1.0 + (int(hashlib.sha1(category.encode()).hexdigest()[:8], 16) % 5) * 0.35
        price = _round2(rng.lognormvariate(3.4, 0.55) * band)
        cost = _round2(price * rng.uniform(0.42, 0.71))

        products.append(
            {
                "product_id": f"P{pid:05d}",
                "sku": f"{category[:3].upper()}-{subcategory[:3].upper()}-{pid:05d}",
                "product_name": f"{subcategory[:-1] if subcategory.endswith('s') else subcategory} "
                                f"{rng.choice(['Pro', 'Lite', 'Studio', 'Classic', 'Max', 'Mini'])} "
                                f"{rng.randint(1, 9)}",
                "category": category,
                "subcategory": subcategory,
                "unit_price": price,
                "unit_cost": cost,
                "is_active": "true" if rng.random() > 0.06 else "false",
            }
        )

    return products


# --------------------------------------------------------------------------
# customers
# --------------------------------------------------------------------------

def enrich_customers(
    rng: random.Random,
    customers: list[dict],
    anchor: dt.date,
    months: int,
) -> tuple[list[dict], list[dict]]:
    """Attach signup dates and tiers; emit the SCD2 change log.

    Returns (customers, change_log). The change log is what makes the SCD2
    dimension produce real history — a snapshot over a static source yields one
    version per customer and its tests become tautologies.
    """
    window_start = anchor - dt.timedelta(days=months * 30)
    change_log: list[dict] = []

    for cust in customers:
        # Signups spread across the window, weighted toward more recent months so
        # cohort sizes grow over time the way a real business does.
        skew = rng.random() ** 0.75
        signup_offset = int(skew * months * 30)
        signup = window_start + dt.timedelta(days=signup_offset)

        cust["signup_date"] = signup.isoformat()
        cust["loyalty_tier"] = _weighted(rng, C.TIER_WEIGHTS)
        cust["segment"] = "new"
        cust["is_deleted"] = "false"
        cust["updated_at"] = f"{signup.isoformat()}T09:00:00+00:00"

    by_id = {c["customer_id"]: c for c in customers}

    # The designated SCD2 demo customer: three backdated tier transitions. Phase 4
    # asserts this customer has exactly three dim_customer versions.
    demo = by_id.get(C.SCD2_DEMO_CUSTOMER_ID)
    if demo is not None:
        # Ensure the demo customer signed up before its first transition.
        earliest = anchor - dt.timedelta(days=C.SCD2_DEMO_TRANSITIONS[0][0] + 30)
        demo["signup_date"] = earliest.isoformat()
        demo["loyalty_tier"] = C.SCD2_DEMO_TRANSITIONS[0][1]

        for days_ago, old, new in C.SCD2_DEMO_TRANSITIONS:
            changed = anchor - dt.timedelta(days=days_ago)
            change_log.append(
                {
                    "customer_id": C.SCD2_DEMO_CUSTOMER_ID,
                    "changed_at": f"{changed.isoformat()}T12:00:00+00:00",
                    "field": "loyalty_tier",
                    "old_value": old,
                    "new_value": new,
                }
            )
        demo["loyalty_tier"] = C.SCD2_DEMO_TRANSITIONS[-1][2]
        demo["updated_at"] = change_log[-1]["changed_at"]

    # A hard delete, so hard_deletes='new_record' has something to act on.
    deleted = by_id.get(C.HARD_DELETE_CUSTOMER_ID)
    if deleted is not None:
        when = anchor - dt.timedelta(days=C.HARD_DELETE_DAYS_AGO)
        deleted["is_deleted"] = "true"
        deleted["updated_at"] = f"{when.isoformat()}T12:00:00+00:00"
        change_log.append(
            {
                "customer_id": C.HARD_DELETE_CUSTOMER_ID,
                "changed_at": f"{when.isoformat()}T12:00:00+00:00",
                "field": "is_deleted",
                "old_value": "false",
                "new_value": "true",
            }
        )

    change_log.sort(key=lambda r: (r["changed_at"], r["customer_id"]))
    return customers, change_log


# --------------------------------------------------------------------------
# orders, items, payments
# --------------------------------------------------------------------------

def make_orders(
    rng: random.Random,
    customers: list[dict],
    products: list[dict],
    anchor: dt.date,
    months: int,
) -> tuple[list[dict], list[dict], list[dict]]:
    orders: list[dict] = []
    items: list[dict] = []
    payments: list[dict] = []

    active_products = [p for p in products if p["is_active"] == "true"]
    order_n = 0
    payment_n = 0

    for cust in customers:
        signup = dt.date.fromisoformat(cust["signup_date"])
        days_available = (anchor - signup).days
        if days_available < 1:
            continue

        # Pareto order frequency: most customers order once or twice, a small
        # tail orders often. Clamped so one customer cannot dominate the dataset.
        #
        # Scaled by tenure so a customer who signed up last month is not expected
        # to have two years of orders. int() truncation on a Pareto draw discards
        # a large share of the mass below 1.0, so the scale factor compensates —
        # tuned against AVG_ORDERS_PER_ACTIVE_CUSTOMER rather than guessed.
        raw = rng.paretovariate(C.ORDER_FREQ_PARETO_ALPHA)
        tenure_ratio = days_available / (months * 30)
        n_orders = min(int(raw * tenure_ratio * C.ORDER_FREQ_SCALE), 45)

        for _ in range(n_orders):
            offset = rng.randint(0, days_available)
            order_date = signup + dt.timedelta(days=offset)

            # Seasonality: reject-sample against the month multiplier so peak
            # months genuinely carry more orders.
            if rng.random() > C.MONTH_SEASONALITY[order_date.month] / 1.58:
                continue

            order_n += 1
            order_id = f"O{order_n:07d}"
            status = _weighted(rng, C.ORDER_STATUS_WEIGHTS)
            channel = _weighted(rng, C.CHANNEL_WEIGHTS)
            device = _weighted(rng, C.DEVICE_WEIGHTS)

            # Basket: lognormal target value realised as 1-5 real line items.
            target = rng.lognormvariate(C.BASKET_LOG_MU, C.BASKET_LOG_SIGMA)
            n_lines = min(1 + int(rng.expovariate(0.8)), 5)

            gross = 0.0
            for line_no in range(1, n_lines + 1):
                prod = rng.choice(active_products)
                qty = 1 + int(rng.expovariate(1.6))
                qty = min(qty, 6)
                line_total = _round2(prod["unit_price"] * qty)
                gross += line_total

                items.append(
                    {
                        "order_item_id": f"{order_id}-{line_no}",
                        "order_id": order_id,
                        "line_number": line_no,
                        "product_id": prod["product_id"],
                        "quantity": qty,
                        "unit_price": prod["unit_price"],
                        "line_amount": line_total,
                    }
                )

            discount = _round2(gross * rng.choice([0.0, 0.0, 0.0, 0.05, 0.10, 0.15]))
            shipping = _round2(0.0 if gross > 75 else rng.uniform(4.95, 12.95))
            tax = _round2((gross - discount) * 0.0825)
            total = _round2(gross - discount + shipping + tax)

            ts = f"{order_date.isoformat()}T{rng.randint(6, 23):02d}:{rng.randint(0, 59):02d}:00+00:00"
            orders.append(
                {
                    "order_id": order_id,
                    "customer_id": cust["customer_id"],
                    "order_ts": ts,
                    "order_date": order_date.isoformat(),
                    "status": status,
                    "channel": channel,
                    "device_type": device,
                    "gross_amount": _round2(gross),
                    "discount_amount": discount,
                    "shipping_amount": shipping,
                    "tax_amount": tax,
                    "total_amount": total,
                    "target_basket": _round2(target),  # kept for defect injection realism
                }
            )

            # Payments. Cancelled orders may never have been captured; a slice of
            # card payments fail once and retry, which is what makes the payment
            # health mart non-trivial.
            attempts = 1
            if status != "cancelled" and rng.random() < 0.11:
                attempts = 2

            for attempt in range(1, attempts + 1):
                payment_n += 1
                if attempt < attempts:
                    pstatus = "failed"
                elif status == "cancelled":
                    pstatus = rng.choice(["failed", "refunded"])
                elif status == "returned":
                    pstatus = "refunded"
                elif rng.random() < 0.004:
                    pstatus = "chargeback"
                else:
                    pstatus = "captured"

                payments.append(
                    {
                        "payment_id": f"PAY{payment_n:07d}",
                        "order_id": order_id,
                        "attempt_number": attempt,
                        "payment_method": _weighted(rng, C.PAYMENT_METHOD_WEIGHTS),
                        "status": pstatus,
                        "amount": total,
                        "processed_ts": ts,
                        "failure_reason": (
                            rng.choice(
                                ["insufficient_funds", "do_not_honor", "expired_card", "network_timeout"]
                            )
                            if pstatus == "failed"
                            else ""
                        ),
                    }
                )

    for o in orders:
        o.pop("target_basket", None)

    return orders, items, payments


# --------------------------------------------------------------------------
# web events
# --------------------------------------------------------------------------

def make_web_events(
    rng: random.Random,
    customers: list[dict],
    products: list[dict],
    orders: list[dict],
    anchor: dt.date,
) -> list[dict]:
    """Sessions with a realistic funnel.

    Purchase events are emitted only for sessions tied to a real order, so the
    web funnel's purchase step reconciles against fact_orders instead of being an
    independent random number that never matches.
    """
    events: list[dict] = []
    active_products = [p for p in products if p["is_active"] == "true"]
    event_n = 0

    # Sessions that converted: one per order, carrying that order's id.
    for order in orders:
        session_id = "S" + hashlib.sha1(order["order_id"].encode()).hexdigest()[:14]
        base_ts = dt.datetime.fromisoformat(order["order_ts"])
        steps = ["page_view", "search", "product_view", "add_to_cart", "begin_checkout", "purchase"]

        for i, etype in enumerate(steps):
            event_n += 1
            events.append(
                {
                    "event_id": f"E{event_n:08d}",
                    "session_id": session_id,
                    "customer_id": order["customer_id"],
                    "event_ts": (base_ts - dt.timedelta(minutes=(len(steps) - i) * 3)).isoformat(),
                    "event_type": etype,
                    "product_id": rng.choice(active_products)["product_id"]
                    if etype in ("product_view", "add_to_cart")
                    else "",
                    "order_id": order["order_id"] if etype == "purchase" else "",
                    "channel": order["channel"],
                    "device_type": order["device_type"],
                }
            )

    # Sessions that did not convert: they drop out somewhere in the funnel. This
    # is what gives the funnel mart a real shape rather than 100% conversion.
    n_abandoned = int(len(orders) * 2.4)
    for _ in range(n_abandoned):
        cust = rng.choice(customers)
        signup = dt.date.fromisoformat(cust["signup_date"])
        span = (anchor - signup).days
        if span < 1:
            continue
        when = signup + dt.timedelta(days=rng.randint(0, span))
        base_ts = dt.datetime.combine(when, dt.time(rng.randint(6, 23), rng.randint(0, 59)))
        session_id = "S" + hashlib.sha1(
            f"{cust['customer_id']}{when}{rng.random()}".encode()
        ).hexdigest()[:14]

        depth = rng.choices([1, 2, 3, 4], weights=[0.42, 0.30, 0.19, 0.09], k=1)[0]
        steps = ["page_view", "product_view", "add_to_cart", "begin_checkout"][:depth]
        channel = _weighted(rng, C.CHANNEL_WEIGHTS)
        device = _weighted(rng, C.DEVICE_WEIGHTS)

        for i, etype in enumerate(steps):
            event_n += 1
            events.append(
                {
                    "event_id": f"E{event_n:08d}",
                    "session_id": session_id,
                    "customer_id": cust["customer_id"],
                    "event_ts": (base_ts + dt.timedelta(minutes=i * 2)).isoformat(),
                    "event_type": etype,
                    "product_id": rng.choice(active_products)["product_id"]
                    if etype in ("product_view", "add_to_cart")
                    else "",
                    "order_id": "",
                    "channel": channel,
                    "device_type": device,
                }
            )

    events.sort(key=lambda e: e["event_ts"])
    return events
