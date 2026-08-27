"""The dashboard's contract with the warehouse.

Three things are asserted here and none of them is "does Streamlit render".

**The read boundary.** CONTRACTS.md §8 says the dashboard reads only `gold` and
only via `dashboard/metrics.py`. That is enforceable by reading the source, and
enforcing it is the whole reason the star schema is worth building — a dashboard
that reaches past the marts has become a second, untested transform layer that
is free to disagree with dbt about what revenue means.

**The additivity rule.** `nadd_` is a naming convention, and a convention
nothing checks is a comment. `sum(nadd_aov)` across six channels gives roughly
4.7x the true figure on this data and renders as a perfectly plausible chart
line, so the test asserts the difference rather than trusting the prefix.

**The freshness banner.** §7 requires the cache key to be
`meta.pipeline_run_log.completed_at` and requires a visible warning when it is
unavailable — never a silent fallback constant, which hides a broken pipeline
behind numbers that look fine.

What is *not* here is rendering. Streamlit writes an uncaught exception into the
page rather than failing the process, so a unit test cannot see a broken panel;
`dashboard/screenshots.py` clicks every tab and fails on Streamlit's exception
block, and it is what caught the Decimal bug these tests all passed through.
"""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "dashboard"

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# the read boundary — source inspection, no database
# ---------------------------------------------------------------------------

# Anything that looks like a query against a schema. Deliberately broad: the
# rule is that the app file contains no SQL at all, so a near-miss should fail.
SQL_SHAPED = re.compile(r"\b(select\s+.+\s+from|insert\s+into|update\s+\w+\s+set)\b", re.I)


def test_the_app_contains_no_sql():
    """Every query lives in `metrics.py`. This is that rule, enforced."""
    offenders = []
    for path in DASHBOARD.glob("*.py"):
        if path.name == "metrics.py":
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#") or stripped.startswith('"'):
                continue
            if SQL_SHAPED.search(line):
                offenders.append(f"{path.name}:{number}: {stripped[:70]}")
    assert not offenders, "SQL outside dashboard/metrics.py:\n" + "\n".join(offenders)


def test_metrics_queries_only_gold_and_meta():
    """`silver`, `secure` and `oltp` must not appear.

    `meta` is allowed and is not a widening: §7 puts the freshness watermark and
    the DQ results there, and the dashboard is specified to read both. `gold` is
    the analytical boundary; `meta` is the operational one.
    """
    source = (DASHBOARD / "metrics.py").read_text(encoding="utf-8")
    for schema in ("silver.", "secure.", "oltp.", "gold_stg.", "gold_int."):
        assert schema not in source, f"metrics.py reaches into {schema}"
    assert "gold." in source and "meta." in source


def test_every_contracted_mart_is_named_by_a_metrics_function():
    """All seven from §8, each with a caller. A mart nothing reads is dead
    weight in the DAG, and one the dashboard needs but does not query is a
    panel that will be written against `silver` by whoever needs it next."""
    from dashboard import metrics

    source = (DASHBOARD / "metrics.py").read_text(encoding="utf-8")
    for mart in metrics.MARTS:
        assert f"gold.{mart}" in source, f"{mart} is declared but never queried"


# ---------------------------------------------------------------------------
# additivity
# ---------------------------------------------------------------------------


def test_summing_a_non_additive_measure_gives_a_visibly_wrong_answer():
    """The concrete reason for the `nadd_` prefix.

    Not a philosophical point: on this data, summing `nadd_aov` across the six
    channels of a day produces about 4.7 times the true average order value.
    That is a number a chart will happily draw. The assertion is that the two
    differ substantially — pinning the exact ratio would make this fail on a
    reseed for no reason.
    """
    from dashboard import metrics

    _skip_without_gold()
    low, high = metrics.date_bounds()
    frame = metrics.daily_sales(low, high)

    wrong = frame.groupby("date_day")["nadd_aov"].sum().mean()
    right = metrics.recompute_aov(
        frame.groupby("date_day", as_index=False)[["revenue", "revenue_orders"]].sum()
    ).mean()

    assert wrong > right * 2, (
        f"summing nadd_aov gave {wrong:.2f} and recomputing gave {right:.2f} — "
        f"if those are close, this mart is no longer at (day, channel) grain "
        f"and the test has stopped asserting anything"
    )


def test_recomputing_a_rate_survives_a_zero_denominator():
    """A day with no attempts must produce a null rate, not a ZeroDivisionError
    and not a zero — a 0% authorisation rate on a day nobody tried to pay is a
    claim about the payment provider."""
    import pandas as pd
    from dashboard import metrics

    result = metrics.recompute_rate(pd.Series([5, 0]), pd.Series([10, 0]))
    assert result.iloc[0] == 50.0
    assert pd.isna(result.iloc[1])


def test_the_funnel_never_widens():
    """Counted per session, so each step is a subset of the one before it.

    True by construction in `int_session_funnel` — which is a claim about code
    somebody will edit.
    """
    from dashboard import metrics

    _skip_without_gold()
    low, high = metrics.date_bounds()
    steps = metrics.funnel_steps(metrics.web_funnel(low, high))
    counts = steps["sessions"].tolist()
    assert counts == sorted(counts, reverse=True), f"funnel widens: {steps.to_dict('records')}"


# ---------------------------------------------------------------------------
# freshness
# ---------------------------------------------------------------------------


def test_the_watermark_comes_from_completed_runs_only():
    """A run still in flight has a null `completed_at`, so it cannot advance the
    watermark — which is what stops a half-finished load presenting itself as
    fresh data."""
    from dashboard import metrics

    source = (DASHBOARD / "metrics.py").read_text(encoding="utf-8")
    assert "completed_at IS NOT NULL" in source
    assert "meta.pipeline_run_log" in source

    mark = metrics.watermark()
    if mark.available:
        assert mark.completed_at.tzinfo is not None, "a naive watermark cannot be compared to now()"


def test_an_unavailable_watermark_is_not_reported_as_merely_stale():
    """Two different failures needing two different responses.

    "The pipeline has never completed" and "the data is six hours old" are not
    the same message, and collapsing them would render the first as the second.
    """
    from dashboard.metrics import Watermark

    missing = Watermark(completed_at=None, step=None, runs_in_flight=0)
    assert not missing.available
    assert missing.age() is None
    assert not missing.stale(dt.timedelta(hours=1)), (
        "an absent watermark reported as stale would show the milder banner"
    )


def test_staleness_is_measured_against_the_watermark_not_the_clock():
    from dashboard.metrics import Watermark

    now = dt.datetime(2026, 8, 27, 12, 0, tzinfo=dt.UTC)
    fresh = Watermark(now - dt.timedelta(hours=1), "load_warehouse", 0)
    old = Watermark(now - dt.timedelta(hours=30), "load_warehouse", 0)

    assert not fresh.stale(dt.timedelta(hours=6), now=now)
    assert old.stale(dt.timedelta(hours=6), now=now)
    assert old.age(now=now) == dt.timedelta(hours=30)


def test_the_app_shows_a_banner_rather_than_a_fallback_constant():
    """§7's actual requirement: never a silent fallback.

    Checked by source rather than by rendering, because the failure mode is an
    *absent* banner and a rendering test cannot assert the absence of something
    it was not told to look for.
    """
    source = (DASHBOARD / "app.py").read_text(encoding="utf-8")
    assert "st.error" in source and "st.warning" in source
    assert "freshness_banner" in source
    # The cache key threads the watermark into every loader. If this stops being
    # true the panels start caching on nothing and never invalidate.
    assert "@st.cache_data" in source
    assert source.count("def load_") >= 8


# ---------------------------------------------------------------------------
# the marts themselves
# ---------------------------------------------------------------------------


def _skip_without_gold() -> None:
    from dashboard import metrics

    try:
        metrics.date_bounds()
    except metrics.WarehouseUnavailable as exc:
        pytest.skip(str(exc))


def test_every_loader_returns_numeric_columns_not_decimals():
    """The bug `dashboard/screenshots.py` found and these tests did not.

    psycopg maps Postgres `numeric` to `decimal.Decimal`, which lands in pandas
    as dtype `object`. `.sum()` works on it, so a unit test summing a column
    passes — while `.nlargest()` raises and Plotly renders an empty axis. Seven
    of eight tabs were broken and every test here was green.
    """
    import decimal

    from dashboard import metrics

    _skip_without_gold()
    low, high = metrics.date_bounds()

    frames = {
        "daily_sales": metrics.daily_sales(low, high),
        "customer_rfm": metrics.customer_rfm(),
        "product_performance": metrics.product_performance(low, high),
        "payment_health": metrics.payment_health(low, high),
        "support_health": metrics.support_health(low, high),
    }
    for name, frame in frames.items():
        for column in frame.columns:
            values = frame[column].dropna()
            if values.empty:
                continue
            assert not isinstance(values.iloc[0], decimal.Decimal), (
                f"{name}.{column} is still Decimal — charts and nlargest will fail on it"
            )


def test_no_contracted_mart_reads_back_empty():
    """An empty mart renders as a blank panel, which looks like a filter with no
    matches. The dashboard cannot tell those apart; this can."""
    from dashboard import metrics

    _skip_without_gold()
    low, high = metrics.date_bounds()

    empties = [
        name
        for name, frame in (
            ("mart_daily_sales", metrics.daily_sales(low, high)),
            ("mart_customer_rfm", metrics.customer_rfm()),
            ("mart_cohort_retention", metrics.cohort_retention()),
            ("mart_product_performance", metrics.product_performance(low, high)),
            ("mart_payment_health", metrics.payment_health(low, high)),
            ("mart_web_funnel", metrics.web_funnel(low, high)),
            ("mart_support_health", metrics.support_health(low, high)),
        )
        if frame.empty
    ]
    assert not empties, f"built but empty: {empties}"


def test_date_bounds_come_from_the_data_not_the_clock():
    """The generator writes to a fixed anchor. A "last 30 days" default would
    open on an empty chart the moment the demo is a month old."""
    from dashboard import metrics

    _skip_without_gold()
    low, high = metrics.date_bounds()
    assert low < high
    # The seed covers roughly two years; anything much shorter means the marts
    # are being filtered somewhere they should not be.
    assert (high - low).days > 300
