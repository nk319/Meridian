"""Data quality suites.

The pure tests assert that the check *generators* are sound and that the
distribution checks can actually fail. That second one matters more than it
looks: a Pandera schema with a typo in a lambda passes everything, and a suite
that cannot fail is indistinguishable from a suite that found nothing wrong.
"""

from __future__ import annotations

import pandas as pd
import pandera.errors
import pytest

from meridian.dq import suites
from meridian.dq.checks import CheckResult, SqlCheck, invariant, referential, unique_key
from meridian.dq.schemas import SPECS

# ---------------------------------------------------------------------------
# check semantics
# ---------------------------------------------------------------------------


def _result(scanned: int, failed: int, **kw) -> CheckResult:
    check = SqlCheck(
        name="t", target_table="silver.x", count_sql="", repro_sql="", message="", **kw
    )
    return CheckResult(check=check, rows_scanned=scanned, rows_failed=failed)


def test_status_is_pass_when_within_tolerance():
    assert _result(100, 2, max_failure_pct=3.0).status == "PASS"
    assert _result(100, 4, max_failure_pct=3.0).status == "FAIL"


def test_zero_tolerance_means_any_failure_fails():
    assert _result(100, 0).status == "PASS"
    assert _result(100, 1).status == "FAIL"


def test_only_block_severity_blocks():
    assert _result(100, 50, severity="BLOCK").blocking is True
    assert _result(100, 50, severity="WARN").blocking is False


def test_empty_table_does_not_divide_by_zero():
    assert _result(0, 0).failure_pct == 0.0
    assert _result(0, 0).status == "PASS"


def test_unknown_severity_is_refused():
    with pytest.raises(ValueError, match="severity"):
        SqlCheck(
            name="t", target_table="x", count_sql="", repro_sql="", message="", severity="LOUD"
        )


# ---------------------------------------------------------------------------
# generated SQL
# ---------------------------------------------------------------------------


def test_referential_check_names_and_targets():
    check = referential("silver.orders", "customer_id", "silver.customers", "customer_id")
    assert check.name == "fk_orders_customer_id"
    assert check.target_table == "silver.orders"
    # A NULL foreign key is absence, not a violation — an anonymous web event
    # has no customer and that is data rather than a defect.
    assert "IS NOT NULL" in check.count_sql


def test_invariant_is_written_as_the_rule_not_the_failure():
    """`holds_when` states what should be true; the generator negates it.

    Writing the failure condition by hand in twenty checks is how a double
    negative eventually slips in and a check starts asserting its own opposite.
    """
    check = invariant("t", "silver.orders", "total_amount >= 0", "totals are non-negative")
    assert "NOT (total_amount >= 0)" in check.count_sql
    assert "NOT (total_amount >= 0)" in check.repro_sql


def test_every_check_carries_a_reproduction():
    """CONTRACTS §7 puts repro_sql in the results table so a finding can be
    acted on. A check without one is a number nobody can chase."""
    for name, checks in suites.build().items():
        for check in checks:
            assert check.repro_sql.strip(), f"{name}/{check.name} has no repro_sql"
            assert check.message.strip(), f"{name}/{check.name} has no message"


def test_suite_registry_is_complete():
    built = suites.build()
    for name in suites.SUITE_NAMES:
        if name == "schema":  # Pandera, not SQL — assembled separately
            continue
        assert name in built, name
    assert built["all"], "the `all` suite is empty"
    # `all` is the union of the others, so a new suite is picked up without
    # anyone remembering to add it in two places.
    union = {c.name for n, cs in built.items() if n != "all" for c in cs}
    assert {c.name for c in built["all"]} == union


def test_check_names_are_unique_within_a_suite():
    for name, checks in suites.build().items():
        names = [c.name for c in checks]
        assert len(names) == len(set(names)), f"duplicate check names in {name}"


def test_every_silver_table_has_an_emptiness_check():
    """The check that stops every other check passing vacuously."""
    guarded = {c.target_table for c in suites.VOLUME if c.name.startswith("not_empty")}
    assert guarded == set(suites.SILVER_TABLES)


def test_unique_key_check_counts_surplus_rows_not_groups():
    check = unique_key("silver.orders", ("order_id",))
    assert "sum(n) - count(*)" in check.count_sql


# ---------------------------------------------------------------------------
# the Pandera schemas must be able to fail
# ---------------------------------------------------------------------------

BY_ENTITY = {s.entity: s for s in SPECS}


def _validate(entity: str, frame: pd.DataFrame) -> set[str]:
    """Returns the set of failing check names — empty means the frame passed."""
    try:
        BY_ENTITY[entity].schema.validate(frame, lazy=True)
        return set()
    except pandera.errors.SchemaErrors as exc:
        return set(exc.failure_cases["check"].astype(str))


def _orders(status: list[str], totals: list[float]) -> pd.DataFrame:
    n = len(status)
    return pd.DataFrame(
        {
            "order_id": [f"O{i}" for i in range(n)],
            "customer_id": ["C1"] * n,
            "status": status,
            "channel": ["direct"] * n,
            "device_type": ["mobile"] * n,
            "total_amount": totals,
            "discount_amount": [0.0] * n,
        }
    )


def _healthy_orders() -> pd.DataFrame:
    # 72% delivered / 6% cancelled / 22% shipped, and a right-skewed total.
    status = ["delivered"] * 72 + ["cancelled"] * 6 + ["shipped"] * 22
    totals = [100.0] * 99 + [5000.0]
    return _orders(status, totals)


def test_healthy_orders_frame_passes():
    """The control. Without it, every failure test below could be passing
    because the schema rejects everything."""
    assert _validate("orders", _healthy_orders()) == set()


def test_status_distribution_shift_is_caught():
    """Every row individually valid, the set as a whole wrong.

    This is the case no CHECK constraint can see, and the reason these schemas
    exist at all.
    """
    failures = _validate("orders", _orders(["pending"] * 100, [100.0] * 100))
    assert any("delivered" in f for f in failures)


def test_lost_right_skew_is_caught():
    flat = _orders(["delivered"] * 72 + ["cancelled"] * 6 + ["shipped"] * 22, [100.0] * 100)
    assert "order totals lost their right skew" in _validate("orders", flat)


def test_invalid_enum_value_is_caught():
    bad = _healthy_orders()
    bad.loc[0, "status"] = "teleported"
    assert _validate("orders", bad)


def test_duplicate_primary_key_is_caught():
    bad = _healthy_orders()
    bad.loc[1, "order_id"] = bad.loc[0, "order_id"]
    assert _validate("orders", bad)


def _products(prices: list[float], costs: list[float], active: list[bool]) -> pd.DataFrame:
    n = len(prices)
    return pd.DataFrame(
        {
            "product_id": [f"P{i}" for i in range(n)],
            "sku": [f"S{i}" for i in range(n)],
            "category": ["Audio"] * n,
            "is_active": active,
            "unit_price": prices,
            "unit_cost": costs,
        }
    )


def _healthy_products() -> pd.DataFrame:
    # ~89% active, ~43% margin — the measured shape of the catalogue.
    active = [True] * 89 + [False] * 11
    return _products([100.0] * 100, [57.0] * 100, active)


def test_healthy_products_frame_passes():
    assert _validate("products", _healthy_products()) == set()


def test_margin_collapse_is_caught():
    """A pricing bug moves margin first, and every row stays plausible.

    Price and cost are both individually valid at every row; only the spread
    between them changes.
    """
    collapsed = _products([100.0] * 100, [95.0] * 100, [True] * 89 + [False] * 11)
    assert any("margin" in f for f in _validate("products", collapsed))


def test_product_priced_below_cost_is_caught():
    bad = _healthy_products()
    bad.loc[0, "unit_cost"] = 500.0
    assert any("cost" in f or "margin" in f for f in _validate("products", bad))


def test_every_spec_has_a_dataframe_level_check():
    """Column rules duplicate the database's CHECK constraints; the frame-level
    ones are what these schemas add. A spec with none is not earning its cost."""
    for spec in SPECS:
        assert spec.schema.checks, f"{spec.entity} has no dataframe-level checks"


def test_every_spec_selects_the_columns_it_validates():
    """A column named in the schema but absent from the query fails at run time,
    against real data, after the query has already been paid for."""
    for spec in SPECS:
        for column in spec.schema.columns:
            assert column in spec.sql, f"{spec.entity}: {column} is not selected"
