"""Seed data generator.

    python -m meridian.seed --out seeds/

Produces every source the batch ingestion layer reads, split by source system so
that each entity has exactly one owner. That ownership matters: orders are
ingested only from OLTP. An earlier design had orders arriving via four paths at
once, which made the Bronze row-count reconciliation check fail permanently and
made every pipeline run look broken.

    oltp/    customers, orders, order_items, customer_change_log   (clean)
    files/   products, web_events                                  (defects injected)
    restapi/ support_tickets                                       (clean; served by the API)
    vendor/  payments, paginated                                   (defects injected)
    rag/     support ticket corpus as JSONL
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import random
import sys
from pathlib import Path

from . import config as C
from . import defects, entities, tickets, writers
from .identity import PIIRegistry, make_customers


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="meridian.seed", description="Generate Meridian source data")
    p.add_argument("--out", type=Path, default=Path("seeds"), help="output directory")
    p.add_argument("--seed", type=int, default=C.DEFAULT_SEED)
    p.add_argument(
        "--anchor-date",
        type=dt.date.fromisoformat,
        default=C.DEFAULT_ANCHOR,
        help="last date in the generated history (default is fixed for determinism)",
    )
    p.add_argument("--months", type=int, default=C.DEFAULT_MONTHS)
    p.add_argument("--customers", type=int, default=C.DEFAULT_CUSTOMERS)
    p.add_argument("--products", type=int, default=C.DEFAULT_PRODUCTS)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    out: Path = args.out
    rng = random.Random(args.seed)
    registry = PIIRegistry()

    log = lambda step, **kw: print(  # noqa: E731 - deliberate one-liner logger
        json.dumps({"step": step, **kw}), flush=True
    )

    log("start", seed=args.seed, anchor=args.anchor_date.isoformat(), months=args.months)

    # --- generate ---------------------------------------------------------
    products = entities.make_products(rng, args.products)
    customers = make_customers(rng, args.customers, registry)
    customers, change_log = entities.enrich_customers(rng, customers, args.anchor_date, args.months)
    orders, order_items, payments = entities.make_orders(
        rng, customers, products, args.anchor_date, args.months
    )
    web_events = entities.make_web_events(rng, customers, products, orders, args.anchor_date)
    ticket_rows = tickets.make_tickets(rng, customers, orders, args.anchor_date)

    log(
        "generated",
        products=len(products),
        customers=len(customers),
        change_log=len(change_log),
        orders=len(orders),
        order_items=len(order_items),
        payments=len(payments),
        web_events=len(web_events),
        tickets=len(ticket_rows),
    )

    # --- split PII out of the customer record -----------------------------
    # PII lives in its own file so it can be loaded into the `secure` schema and
    # excluded by grant. CONTRACTS.md §10: separation is the enforcement
    # mechanism, the documentation only describes it.
    pii_columns = ["first_name", "last_name", "email", "phone"]
    customer_pii = [
        {"customer_id": c["customer_id"], **{k: c[k] for k in pii_columns}} for c in customers
    ]
    customers_safe = [
        {k: v for k, v in c.items() if k not in pii_columns} for c in customers
    ]

    # --- inject defects into third-party feeds only -----------------------
    defect_manifest: list[dict] = []

    products_dirty, m = defects.inject(
        rng,
        products,
        entity="products",
        enum_columns={"is_active": ["true", "false"]},
        required_columns=["product_name", "category"],
        numeric_columns=["unit_price", "unit_cost"],
    )
    defect_manifest += m

    web_events_dirty, m = defects.inject(
        rng,
        web_events,
        entity="web_events",
        enum_columns={"event_type": C.EVENT_TYPE, "device_type": C.DEVICE_TYPE},
        required_columns=["session_id", "event_type"],
        date_columns=["event_ts"],
    )
    defect_manifest += m

    payments_dirty, m = defects.inject(
        rng,
        payments,
        entity="payments",
        enum_columns={"status": C.PAYMENT_STATUS, "payment_method": C.PAYMENT_METHOD},
        required_columns=["order_id"],
        numeric_columns=["amount"],
        date_columns=["processed_ts"],
    )
    defect_manifest += m

    log("defects_injected", count=len(defect_manifest))

    # --- write ------------------------------------------------------------
    counts = {
        "oltp/customers.csv": writers.write_csv(out / "oltp" / "customers.csv", customers_safe),
        "oltp/orders.csv": writers.write_csv(out / "oltp" / "orders.csv", orders),
        "oltp/order_items.csv": writers.write_csv(out / "oltp" / "order_items.csv", order_items),
        "oltp/customer_change_log.csv": writers.write_csv(
            out / "oltp" / "customer_change_log.csv", change_log
        ),
        "secure/customer_pii.csv": writers.write_csv(
            out / "secure" / "customer_pii.csv", customer_pii
        ),
        "files/products.csv": writers.write_csv(out / "files" / "products.csv", products_dirty),
        "files/web_events.csv": writers.write_csv(
            out / "files" / "web_events.csv", web_events_dirty
        ),
        "restapi/support_tickets.json": writers.write_json(
            out / "restapi" / "support_tickets.json", ticket_rows
        ),
        "rag/support_tickets.jsonl": writers.write_jsonl(
            out / "rag" / "support_tickets.jsonl", ticket_rows
        ),
    }
    vendor_pages = writers.write_vendor_pages(out / "vendor", payments_dirty)
    counts["vendor/payments_page_*.json"] = vendor_pages

    # --- manifests --------------------------------------------------------
    writers.write_json(out / "known_pii_terms.json", registry.as_manifest())
    writers.write_json(out / "defect_manifest.json", defect_manifest)

    manifest = {
        "generated_with": {
            "seed": args.seed,
            "anchor_date": args.anchor_date.isoformat(),
            "months": args.months,
            "customers": args.customers,
            "products": args.products,
        },
        "row_counts": counts,
        "scd2": {
            "demo_customer_id": C.SCD2_DEMO_CUSTOMER_ID,
            "expected_versions": len(C.SCD2_DEMO_TRANSITIONS) + 1,
            "hard_deleted_customer_id": C.HARD_DELETE_CUSTOMER_ID,
        },
        "pii_term_counts": registry.as_manifest()["counts"],
        "defects_injected": len(defect_manifest),
    }
    writers.write_json(out / "manifest.json", manifest)

    log("done", **counts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
