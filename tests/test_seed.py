"""Phase 0 acceptance tests.

These are the checks that make Phase 0 "done" rather than "written". Each maps to
an acceptance criterion in the build plan:

  * the generator is deterministic
  * the clean sources are referentially intact
  * the SCD2 dimension will actually be exercised
  * the PII manifest is non-empty and describes real text
  * defects landed only in third-party feeds, never in OLTP truth
"""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"

# Small volumes: these tests check structure, not scale, and a 300-customer run
# completes in about a second.
GEN_ARGS = ["--customers", "300", "--products", "60", "--months", "24"]


def _generate(out: Path, extra: list[str] | None = None) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "meridian.seed", "--out", str(out), *GEN_ARGS, *(extra or [])],
        cwd=REPO,
        env={"PYTHONPATH": str(SRC), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"generator failed:\n{result.stderr}"


def _read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _digest(directory: Path) -> str:
    """Hash every generated file, so any drift anywhere shows up."""
    h = hashlib.sha256()
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            h.update(path.relative_to(directory).as_posix().encode())
            h.update(path.read_bytes())
    return h.hexdigest()


@pytest.fixture(scope="module")
def seeds(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("seeds")
    _generate(out)
    return out


# --------------------------------------------------------------------------
# determinism
# --------------------------------------------------------------------------


def test_generator_is_deterministic(tmp_path):
    """Same seed, same output — byte for byte.

    Determinism is what makes the acceptance tests in later phases stable. If the
    seed data drifts, every downstream row-count assertion becomes flaky.
    """
    a, b = tmp_path / "a", tmp_path / "b"
    _generate(a)
    _generate(b)
    assert _digest(a) == _digest(b)


def test_different_seed_produces_different_data(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    _generate(a)
    _generate(b, extra=["--seed", "999"])
    assert _digest(a) != _digest(b)


# --------------------------------------------------------------------------
# referential integrity — clean sources only
# --------------------------------------------------------------------------


def test_orders_reference_real_customers(seeds):
    customers = {c["customer_id"] for c in _read_csv(seeds / "oltp" / "customers.csv")}
    orders = _read_csv(seeds / "oltp" / "orders.csv")
    assert orders, "no orders generated"
    orphans = {o["order_id"] for o in orders if o["customer_id"] not in customers}
    assert not orphans, f"{len(orphans)} orders reference a missing customer"


def test_order_items_reference_real_orders(seeds):
    order_ids = {o["order_id"] for o in _read_csv(seeds / "oltp" / "orders.csv")}
    items = _read_csv(seeds / "oltp" / "order_items.csv")
    assert items
    orphans = [i for i in items if i["order_id"] not in order_ids]
    assert not orphans, f"{len(orphans)} order items reference a missing order"


def test_order_totals_reconcile_to_line_items(seeds):
    """gross_amount must equal the sum of its line amounts.

    This is the check that catches a whole class of generator bug, and it is the
    same reconciliation the Silver layer will run in Phase 3.
    """
    items_by_order: dict[str, float] = {}
    for item in _read_csv(seeds / "oltp" / "order_items.csv"):
        items_by_order[item["order_id"]] = items_by_order.get(item["order_id"], 0.0) + float(
            item["line_amount"]
        )

    mismatches = []
    for order in _read_csv(seeds / "oltp" / "orders.csv"):
        expected = items_by_order.get(order["order_id"], 0.0)
        actual = float(order["gross_amount"])
        if abs(expected - actual) > 0.02:  # rounding tolerance
            mismatches.append((order["order_id"], expected, actual))
    assert not mismatches, f"{len(mismatches)} orders do not reconcile, e.g. {mismatches[:3]}"


def test_every_order_has_at_least_one_line(seeds):
    order_ids = {o["order_id"] for o in _read_csv(seeds / "oltp" / "orders.csv")}
    with_lines = {i["order_id"] for i in _read_csv(seeds / "oltp" / "order_items.csv")}
    assert order_ids - with_lines == set()


def test_tickets_reference_real_orders_and_customers(seeds):
    customers = {c["customer_id"] for c in _read_csv(seeds / "oltp" / "customers.csv")}
    orders = {o["order_id"] for o in _read_csv(seeds / "oltp" / "orders.csv")}
    tickets = json.loads((seeds / "restapi" / "support_tickets.json").read_text())
    assert tickets
    for t in tickets:
        assert t["customer_id"] in customers
        assert t["order_id"] in orders


def test_history_spans_the_requested_window(seeds):
    """24 months, not 90 days.

    A short window makes the cohort retention heatmap a three-row triangle and
    collapses NTILE(5) RFM scoring into five identical buckets. The window is a
    correctness property of the dataset, not a nice-to-have.
    """
    dates = sorted(o["order_date"] for o in _read_csv(seeds / "oltp" / "orders.csv"))
    first, last = dates[0], dates[-1]
    span_days = (
        __import__("datetime").date.fromisoformat(last)
        - __import__("datetime").date.fromisoformat(first)
    ).days
    assert span_days > 600, f"history spans only {span_days} days"


# --------------------------------------------------------------------------
# SCD2 will actually be exercised
# --------------------------------------------------------------------------


def test_scd2_demo_customer_has_transitions(seeds):
    """A snapshot over static data yields one version per customer forever.

    Its tests then pass trivially against an empty result set, which is worse
    than no test. These rows are what make the SCD2 assertions in Phase 4 real.
    """
    manifest = json.loads((seeds / "manifest.json").read_text())
    demo_id = manifest["scd2"]["demo_customer_id"]

    changes = [
        c
        for c in _read_csv(seeds / "oltp" / "customer_change_log.csv")
        if c["customer_id"] == demo_id and c["field"] == "loyalty_tier"
    ]
    assert len(changes) == 3, f"expected 3 tier transitions, found {len(changes)}"

    # Transitions must chain: each old_value matches the previous new_value.
    ordered = sorted(changes, key=lambda c: c["changed_at"])
    for prev, nxt in zip(ordered, ordered[1:], strict=False):
        assert prev["new_value"] == nxt["old_value"], "tier transitions do not chain"


def test_scd2_hard_delete_exists(seeds):
    manifest = json.loads((seeds / "manifest.json").read_text())
    deleted_id = manifest["scd2"]["hard_deleted_customer_id"]
    customers = {c["customer_id"]: c for c in _read_csv(seeds / "oltp" / "customers.csv")}
    assert customers[deleted_id]["is_deleted"] == "true"


# --------------------------------------------------------------------------
# PII
# --------------------------------------------------------------------------


def test_pii_manifest_is_populated(seeds):
    manifest = json.loads((seeds / "known_pii_terms.json").read_text())
    assert manifest["counts"]["names"] > 0
    assert manifest["counts"]["emails"] > 0
    assert manifest["counts"]["phones"] > 0


def test_pii_manifest_covers_text_that_actually_appears(seeds):
    """The manifest has to describe the corpus, not just exist.

    Phase 1 masks by dictionary lookup against this manifest. If the manifest and
    the ticket bodies disagree, masking silently does nothing and unmasked names
    reach the vector store — where content-hash skip logic means they are never
    re-embedded and never surface.
    """
    manifest = json.loads((seeds / "known_pii_terms.json").read_text())
    known = set(manifest["names"]) | set(manifest["emails"]) | set(manifest["phones"])

    corpus = (seeds / "rag" / "support_tickets.jsonl").read_text()
    hits = sum(1 for term in known if term in corpus)
    assert hits > 50, f"only {hits} manifest terms appear in the corpus — masking would be a no-op"


def test_pii_columns_are_not_in_the_customers_table(seeds):
    """PII is physically separated so grants can exclude it (CONTRACTS.md §10)."""
    header = _read_csv(seeds / "oltp" / "customers.csv")[0].keys()
    for column in ("first_name", "last_name", "email", "phone"):
        assert column not in header, f"{column} leaked into oltp.customers"

    pii_header = _read_csv(seeds / "secure" / "customer_pii.csv")[0].keys()
    assert "email" in pii_header


# --------------------------------------------------------------------------
# defect routing
# --------------------------------------------------------------------------


def test_defects_exist_in_third_party_feeds(seeds):
    defects = json.loads((seeds / "defect_manifest.json").read_text())
    assert defects, "no defects injected — the DQ layer would have nothing to catch"
    entities = {d["entity"] for d in defects}
    assert entities <= {"products", "web_events", "payments"}


def test_oltp_truth_is_clean(seeds):
    """Defects must never enter OLTP.

    Orders are ingested from OLTP only. An earlier design routed corrupted rows
    through every source at once, which made the Bronze row-count reconciliation
    check fail permanently and made every run look broken.
    """
    for row in _read_csv(seeds / "oltp" / "orders.csv"):
        assert row["customer_id"], "null customer_id in OLTP orders"
        assert row["order_id"], "null order_id in OLTP orders"
        assert float(row["total_amount"]) >= 0, "negative total in OLTP orders"

    for row in _read_csv(seeds / "oltp" / "order_items.csv"):
        assert int(row["quantity"]) > 0
        assert float(row["line_amount"]) >= 0
