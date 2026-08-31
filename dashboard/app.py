"""The Meridian dashboard.

    streamlit run dashboard/app.py
    make dashboard

Reads **only** `gold`, and only through `dashboard/metrics.py`. Not one `SELECT`
appears in this file, and `tests/test_dashboard.py` fails if one does. The
reason is not tidiness: a dashboard that reaches past the marts for "just one
number" has become a second transform layer — in Python, untested, and free to
disagree with dbt about what revenue means. When it does, nobody can tell which
number is wrong.

**Every panel is cached on the pipeline watermark, not on a clock.**
`@st.cache_data` keys on its arguments, so passing
`meta.pipeline_run_log.completed_at` into every loader means a completed
pipeline run invalidates the whole dashboard at once, and a *failed* one does
not — `completed_at` stays null while a run is in flight, so partial data can
never present itself as fresh. A TTL would instead serve pre-load numbers for
its duration and be unable to say it was doing so. CONTRACTS.md §7.

**When the watermark is missing or old, the banner says so.** Never a silent
fallback constant: that hides a broken pipeline behind numbers that look fine.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# The dashboard is run by `streamlit run`, which puts this file's directory on
# sys.path rather than the repository root, so `import dashboard.metrics` fails
# and `import metrics` would work by accident. Naming the root explicitly makes
# the import mean the same thing here, in pytest, and in the container.
ROOT = Path(__file__).resolve().parents[1]
for candidate in (ROOT, ROOT / "src"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from dashboard import metrics  # noqa: E402

STALE_AFTER = dt.timedelta(hours=6)

st.set_page_config(
    page_title="Meridian",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# cached loaders — every one keyed on the watermark
# ---------------------------------------------------------------------------
#
# `_key` is the watermark and is otherwise unused inside each function. That is
# the point: `st.cache_data` hashes its arguments, so a changed watermark is a
# cache miss on every panel simultaneously. Naming it with a leading underscore
# would make Streamlit *skip* it when hashing, which is exactly wrong here — so
# it deliberately does not have one.


@st.cache_data(show_spinner=False)
def load_daily_sales(key, start, end):
    return metrics.daily_sales(start, end)


@st.cache_data(show_spinner=False)
def load_rfm(key):
    return metrics.customer_rfm()


@st.cache_data(show_spinner=False)
def load_cohorts(key):
    return metrics.cohort_retention()


@st.cache_data(show_spinner=False)
def load_products(key, start, end):
    return metrics.product_performance(start, end)


@st.cache_data(show_spinner=False)
def load_payments(key, start, end):
    return metrics.payment_health(start, end)


@st.cache_data(show_spinner=False)
def load_funnel(key, start, end):
    return metrics.web_funnel(start, end)


@st.cache_data(show_spinner=False)
def load_support(key, start, end):
    return metrics.support_health(start, end)


@st.cache_data(show_spinner=False)
def load_dq(key):
    return metrics.data_quality_summary()


@st.cache_data(show_spinner=False)
def load_lag(key):
    return metrics.consumer_lag()


@st.cache_data(show_spinner=False)
def load_bounds(key):
    return metrics.date_bounds()


# ---------------------------------------------------------------------------
# freshness banner
# ---------------------------------------------------------------------------


def freshness_banner() -> metrics.Watermark:
    """Three states, three different messages. Never a silent fallback.

    Missing and stale are separate cases because they need separate responses:
    a missing watermark means the pipeline has never completed, and a stale one
    means it has stopped. Collapsing them would render "this has never worked"
    as "this is a bit old".
    """
    mark = metrics.watermark()

    if not mark.available:
        st.error(
            "**No completed pipeline run.** Every number below is from whatever "
            "is currently in `gold`, and nothing says when it got there. "
            "Run `make pipeline && make gold`.",
            icon="⚠",
        )
        return mark

    age = mark.age()
    stamp = mark.completed_at.strftime("%Y-%m-%d %H:%M UTC")
    hours = age.total_seconds() / 3600

    if mark.stale(STALE_AFTER):
        st.warning(
            f"**Data is {hours:.1f} hours old.** The last successful pipeline "
            f"step (`{mark.step}`) completed at {stamp}, past the "
            f"{STALE_AFTER.total_seconds() / 3600:.0f}-hour freshness bar. "
            f"These numbers are real but they are not current.",
            icon="⚠",
        )
    else:
        st.caption(f"Data as of {stamp} — last step `{mark.step}`, {hours:.1f} hours ago.")

    if mark.runs_in_flight:
        st.info(
            f"{mark.runs_in_flight} pipeline run(s) in flight. The watermark will "
            f"not move until they complete, so nothing below is half-loaded.",
            icon="⏳",
        )
    return mark


def metric_row(items: list[tuple[str, str, str | None]]) -> None:
    for column, (label, value, help_text) in zip(st.columns(len(items)), items, strict=True):
        column.metric(label, value, help=help_text)


def money(value) -> str:
    return f"£{float(value or 0):,.0f}"


# ---------------------------------------------------------------------------
# tabs
# ---------------------------------------------------------------------------


def tab_revenue(key, start, end) -> None:
    frame = load_daily_sales(key, start, end)
    if frame.empty:
        st.info("No orders in this range.")
        return

    daily = frame.groupby("date_day", as_index=False)[
        ["revenue", "orders", "revenue_orders", "customers", "units", "margin"]
    ].sum()
    # Recomputed, never summed. `sum(nadd_aov)` over six channels gives roughly
    # 4.7x the true figure on this data — 660 against 142 — and it renders as a
    # perfectly plausible chart line.
    daily["aov"] = metrics.recompute_aov(daily)

    total_revenue = daily["revenue"].sum()
    total_orders = int(daily["revenue_orders"].sum())
    metric_row(
        [
            ("Revenue", money(total_revenue), "Cancelled and returned orders contribute zero."),
            ("Orders", f"{total_orders:,}", "Revenue-recognised orders only."),
            (
                "AOV",
                f"£{(float(total_revenue) / total_orders if total_orders else 0):,.2f}",
                "Recomputed from revenue ÷ orders. Summing `nadd_aov` would be ~4.7x wrong.",
            ),
            ("Margin", money(daily["margin"].sum()), "Against the current catalogue cost."),
        ]
    )

    st.plotly_chart(
        px.line(
            daily,
            x="date_day",
            y="revenue",
            title="Daily revenue",
            labels={"date_day": "", "revenue": "revenue"},
        ),
        use_container_width=True,
    )

    left, right = st.columns(2)
    by_channel = frame.groupby("channel", as_index=False)[["revenue", "revenue_orders"]].sum()
    by_channel["aov"] = metrics.recompute_aov(by_channel)
    left.plotly_chart(
        px.bar(
            by_channel.sort_values("revenue"),
            x="revenue",
            y="channel",
            orientation="h",
            title="Revenue by channel",
        ),
        use_container_width=True,
    )
    right.plotly_chart(
        px.bar(
            by_channel.sort_values("aov"),
            x="aov",
            y="channel",
            orientation="h",
            title="AOV by channel (recomputed, not summed)",
        ),
        use_container_width=True,
    )


def tab_customers(key) -> None:
    frame = load_rfm(key)
    if frame.empty:
        st.info("`mart_customer_rfm` is empty.")
        return

    live = frame[~frame["is_deleted"].astype(bool)]
    metric_row(
        [
            ("Customers with orders", f"{len(live):,}", None),
            (
                "Champions",
                f"{(live['rfm_segment'] == 'champion').sum():,}",
                "R, F and M all in the top two quintiles.",
            ),
            (
                "At risk",
                f"{(live['rfm_segment'] == 'at_risk').sum():,}",
                "Frequent buyers who have gone quiet.",
            ),
            ("Median spend", money(live["monetary"].median()), None),
        ]
    )

    left, right = st.columns([2, 3])
    counts = live["rfm_segment"].value_counts().reset_index()
    counts.columns = ["segment", "customers"]
    left.plotly_chart(
        px.bar(
            counts.sort_values("customers"),
            x="customers",
            y="segment",
            orientation="h",
            title="RFM segments",
        ),
        use_container_width=True,
    )

    # R against F, sized by spend. Quintiles rather than raw values on both
    # axes, because RFM scores are ranks within a population — the whole reason
    # `mart_customer_rfm` uses `ntile` rather than fixed thresholds.
    grid = live.groupby(["r_score", "f_score"], as_index=False).agg(
        customers=("customer_id", "count"), spend=("monetary", "sum")
    )
    right.plotly_chart(
        px.density_heatmap(
            grid,
            x="r_score",
            y="f_score",
            z="customers",
            histfunc="sum",
            title="Recency × frequency (customers)",
            labels={"r_score": "recency quintile", "f_score": "frequency quintile"},
        ),
        use_container_width=True,
    )


def tab_retention(key) -> None:
    frame = load_cohorts(key)
    if frame.empty:
        st.info("`mart_cohort_retention` is empty.")
        return

    grid = frame.pivot(index="cohort_month", columns="months_since", values="nadd_retention_pct")
    st.plotly_chart(
        px.imshow(
            grid,
            labels={"x": "months since first order", "y": "cohort", "color": "% retained"},
            title="Retention by acquisition cohort",
            aspect="auto",
            color_continuous_scale="Blues",
        ),
        use_container_width=True,
    )
    st.caption(
        "Cohorts are the month of a customer's **first order**, not their signup. "
        "Signup cohorts measure how good marketing was at collecting registrations; "
        "order cohorts measure whether people came back. The triangle is not "
        "squared off — a cohort acquired last month has no month-6 number, and "
        "drawing a zero there would show a cliff that is an artefact of the calendar."
    )


def tab_products(key, start, end) -> None:
    frame = load_products(key, start, end)
    if frame.empty:
        st.info("No product activity in this range.")
        return

    totals = frame.groupby(["product_id", "product_name", "category"], as_index=False)[
        ["units", "revenue", "margin", "orders"]
    ].sum()
    totals["margin_pct"] = metrics.recompute_rate(totals["margin"], totals["revenue"])
    top = totals.nlargest(15, "revenue")

    left, right = st.columns(2)
    left.plotly_chart(
        px.bar(
            top.sort_values("revenue"),
            x="revenue",
            y="product_name",
            orientation="h",
            title="Top 15 products by revenue",
            hover_data=["category", "units"],
        ),
        use_container_width=True,
    )
    by_category = frame.groupby("category", as_index=False)[["revenue", "margin"]].sum()
    by_category["margin_pct"] = metrics.recompute_rate(
        by_category["margin"], by_category["revenue"]
    )
    right.plotly_chart(
        px.bar(
            by_category.sort_values("revenue"),
            x="revenue",
            y="category",
            orientation="h",
            title="Revenue by category",
            hover_data=["margin_pct"],
        ),
        use_container_width=True,
    )
    st.caption(
        "Built on `fact_order_items`. At order-header grain this panel cannot "
        "exist — an order does not have a product — which is the argument for "
        "splitting the fact grain."
    )


def tab_payments(key, start, end) -> None:
    frame = load_payments(key, start, end)
    if frame.empty:
        st.info("No payment attempts in this range.")
        return

    attempts = int(frame["attempts"].sum())
    captures = int(frame["captures"].sum())
    metric_row(
        [
            ("Attempts", f"{attempts:,}", "One row per attempt, not per paid order."),
            ("Captured", money(frame["captured_amount"].sum()), None),
            (
                "Auth rate",
                f"{(100.0 * captures / attempts if attempts else 0):.1f}%",
                "Recomputed. Adding yesterday's 92% to today's 91% gives 183%.",
            ),
            ("Chargebacks", f"{int(frame['chargebacks'].sum()):,}", None),
        ]
    )

    daily = frame.groupby("date_day", as_index=False)[["attempts", "captures"]].sum()
    daily["auth_rate"] = metrics.recompute_rate(daily["captures"], daily["attempts"])
    st.plotly_chart(
        px.line(
            daily,
            x="date_day",
            y="auth_rate",
            title="Authorisation rate (recomputed daily)",
            labels={"date_day": "", "auth_rate": "%"},
        ),
        use_container_width=True,
    )

    # The failure mix, unpacked from the jsonb column. A column per reason would
    # need a migration every time the provider adds one.
    reasons: dict[str, int] = {}
    for blob in frame["failure_reason_counts"]:
        for reason, count in (blob or {}).items():
            reasons[reason] = reasons.get(reason, 0) + int(count)
    if reasons:
        mix = pd.DataFrame(sorted(reasons.items()), columns=["reason", "failures"])
        st.plotly_chart(
            px.bar(
                mix.sort_values("failures"),
                x="failures",
                y="reason",
                orientation="h",
                title="Failure reasons",
            ),
            use_container_width=True,
        )


def tab_funnel(key, start, end) -> None:
    frame = load_funnel(key, start, end)
    if frame.empty:
        st.info("No sessions in this range.")
        return

    steps = metrics.funnel_steps(frame)
    left, right = st.columns([3, 2])
    left.plotly_chart(
        px.funnel(steps, x="sessions", y="step", title="View → cart → checkout → purchase"),
        use_container_width=True,
    )
    right.dataframe(steps, use_container_width=True, hide_index=True)
    st.caption(
        "Counted per **session**, not per event. A session that adds four items "
        "to a cart produces four `add_to_cart` events and one `begin_checkout`, "
        "so an event-counted funnel can show more carts than views. Anonymous "
        "sessions are included: they are most of the top of a real funnel, and "
        "excluding them inflates every rate below."
    )


def tab_support(key, start, end) -> None:
    frame = load_support(key, start, end)
    if frame.empty:
        st.info("No tickets in this range.")
        return

    tickets = int(frame["tickets"].sum())
    enriched = int(frame["ai_enriched"].sum())
    agreements = int(frame["ai_intent_agreements"].sum())

    metric_row(
        [
            ("Tickets", f"{tickets:,}", None),
            (
                "Resolved",
                f"{(100.0 * frame['resolved_tickets'].sum() / tickets if tickets else 0):.1f}%",
                None,
            ),
            (
                "Negative",
                f"{(100.0 * frame['negative_tickets'].sum() / tickets if tickets else 0):.1f}%",
                "Source-system sentiment — the ground truth the model is scored against.",
            ),
            (
                "AI coverage",
                f"{(100.0 * enriched / tickets if tickets else 0):.1f}%",
                "Tickets the LLM has classified. 0% without an ANTHROPIC_API_KEY.",
            ),
        ]
    )

    if enriched:
        st.success(
            f"**AI intent accuracy: {100.0 * agreements / enriched:.1f}%** over "
            f"{enriched:,} enriched tickets, scored against labels the model was "
            f"never shown.",
            icon="✓",
        )
    else:
        # Explicitly not "0% accurate". That would be a claim about the model
        # rather than about the configuration, and it is the distinction
        # `mart_support_health` divides by `ai_enriched` to preserve.
        st.info(
            "No tickets have been enriched, so accuracy is **unknown** rather "
            "than zero — set `ANTHROPIC_API_KEY` and run `make rag-enrich`. The "
            "join, the agreement flags and the null-safe denominators are all "
            "exercised; only the model call is missing.",
            icon="ℹ",
        )

    left, right = st.columns(2)
    by_intent = frame.groupby("intent", as_index=False)[["tickets", "negative_tickets"]].sum()
    by_intent["negative_pct"] = metrics.recompute_rate(
        by_intent["negative_tickets"], by_intent["tickets"]
    )
    left.plotly_chart(
        px.bar(
            by_intent.sort_values("tickets"),
            x="tickets",
            y="intent",
            orientation="h",
            title="Volume by intent",
            hover_data=["negative_pct"],
        ),
        use_container_width=True,
    )
    daily = frame.groupby("date_day", as_index=False)[["tickets", "resolved_tickets"]].sum()
    right.plotly_chart(
        px.line(
            daily,
            x="date_day",
            y=["tickets", "resolved_tickets"],
            title="Tickets opened and resolved",
            labels={"date_day": "", "value": "tickets"},
        ),
        use_container_width=True,
    )


def tab_operations(key) -> None:
    st.subheader("Data quality")
    dq = load_dq(key)
    if dq.empty:
        st.info("`meta.dq_check_results` is empty — run `make dq`.")
    else:
        failing = dq[dq["status"] == "FAIL"]
        blocking = failing[failing["severity"] == "BLOCK"]
        metric_row(
            [
                ("Checks", f"{len(dq):,}", "Latest outcome per check, across all three sources."),
                ("Failing", f"{len(failing):,}", None),
                ("Blocking", f"{len(blocking):,}", "A BLOCK failure stops the pipeline."),
                (
                    "Sources",
                    ", ".join(sorted(dq["source"].unique())),
                    "Pandera, custom SQL and dbt all land in one table.",
                ),
            ]
        )
        st.dataframe(
            dq[
                [
                    "source",
                    "check_name",
                    "target_table",
                    "severity",
                    "status",
                    "rows_failed",
                    "failure_pct",
                    "checked_at",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Streaming")
    lag = load_lag(key)
    if lag.empty:
        st.info("No consumer offsets recorded — run `make stream-demo`.")
    else:
        st.dataframe(lag, use_container_width=True, hide_index=True)
        st.caption(
            "One reading cannot tell a dead consumer from a slow one — both show "
            "lag, and only the trend separates them. `meta.kafka_consumer_offsets` "
            "keeps `observed_at` in its primary key so the history accumulates."
        )


# ---------------------------------------------------------------------------
# page
# ---------------------------------------------------------------------------


def main() -> None:
    st.title("◆ Meridian")

    try:
        mark = freshness_banner()
        key = mark.completed_at.isoformat() if mark.available else "no-watermark"
        low, high = load_bounds(key)
    except metrics.WarehouseUnavailable as exc:
        st.error(f"**The warehouse is unavailable.** {exc}", icon="⛔")
        st.stop()
        return

    with st.sidebar:
        st.header("Filters")
        chosen = st.date_input(
            "Date range",
            value=(low, high),
            min_value=low,
            max_value=high,
            help=(
                "Bounded by what the marts contain, not by the wall clock. The "
                "generator writes to a fixed anchor, so a 'last 30 days' default "
                "would open on an empty chart once the demo is a month old."
            ),
        )
        start, end = chosen if isinstance(chosen, tuple) and len(chosen) == 2 else (low, high)

        st.divider()
        st.caption(
            "Every panel reads `gold` through `dashboard/metrics.py` and nothing "
            "else. Panels are cached on `meta.pipeline_run_log.completed_at`, so "
            "a completed run invalidates all of them at once and a failed one "
            "invalidates none."
        )

    tabs = st.tabs(
        [
            "Revenue",
            "Customers",
            "Retention",
            "Products",
            "Payments",
            "Funnel",
            "Support & AI",
            "Operations",
        ]
    )
    with tabs[0]:
        tab_revenue(key, start, end)
    with tabs[1]:
        tab_customers(key)
    with tabs[2]:
        tab_retention(key)
    with tabs[3]:
        tab_products(key, start, end)
    with tabs[4]:
        tab_payments(key, start, end)
    with tabs[5]:
        tab_funnel(key, start, end)
    with tabs[6]:
        tab_support(key, start, end)
    with tabs[7]:
        tab_operations(key)


main()
