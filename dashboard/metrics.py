"""Every query the dashboard makes. CONTRACTS.md §8.

**The dashboard reads only `gold`, and only through this module.** Not a style
rule — it is the thing that makes the star schema worth having. A dashboard
that reaches into `silver` for "just one number" has quietly become a second
transform layer, written in Python, untested, and disagreeing with the marts
about what revenue means. Every function here names a mart, and
`tests/test_dashboard.py` fails if a `st.` file contains SQL.

**Non-additive measures are recomputed, never summed.** `mart_daily_sales` is
at (day, channel) grain, so a chart of daily revenue across all channels sums
`revenue` and `orders` — and must *divide* to get AOV rather than summing
`nadd_aov`. That is what the prefix is for, and this module is where honouring
it actually happens. `recompute_aov` and its siblings exist so no caller has to
remember.

**The cache key is `meta.pipeline_run_log.completed_at`**, per §7. Caching on a
clock means the dashboard serves numbers from before the last load for up to
the TTL and cannot tell you it is doing so; caching on the watermark means a
completed pipeline run invalidates every panel at once, and a *failed* one does
not — because `completed_at` stays null while a run is in flight, so a
half-finished load cannot present itself as fresh data.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import pandas as pd

from meridian.db import UpstreamUnavailable, connect

# Only these. A query naming anything else is a bug this module exists to
# prevent, and `tests/test_dashboard.py` checks the list against the warehouse.
MARTS = (
    "mart_daily_sales",
    "mart_customer_rfm",
    "mart_cohort_retention",
    "mart_product_performance",
    "mart_payment_health",
    "mart_web_funnel",
    "mart_support_health",
)


class WarehouseUnavailable(RuntimeError):
    """Postgres is unreachable, or `gold` has not been built.

    Distinct from an empty result so the dashboard can say which. "No data for
    this filter" and "the warehouse is down" look identical on a chart and
    require completely different responses.
    """


@dataclass(frozen=True)
class Watermark:
    """When the pipeline last finished, and whether that is recent enough.

    `stale` is a property of the data rather than of the clock: it compares the
    last completed run against `max_age`, so a demo left running overnight says
    so instead of quietly showing yesterday's numbers as today's.
    """

    completed_at: dt.datetime | None
    step: str | None
    runs_in_flight: int

    @property
    def available(self) -> bool:
        return self.completed_at is not None

    def age(self, now: dt.datetime | None = None) -> dt.timedelta | None:
        if self.completed_at is None:
            return None
        return (now or dt.datetime.now(dt.UTC)) - self.completed_at

    def stale(self, max_age: dt.timedelta, now: dt.datetime | None = None) -> bool:
        age = self.age(now)
        # Unavailable is not stale — it is worse, and the banner says so
        # separately. Collapsing the two would let "the pipeline has never run"
        # render as "the data is a bit old".
        return age is not None and age > max_age


def _query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Run one statement as `analytics_ro` and return a frame.

    A fresh connection per call rather than a pooled one. Streamlit reruns the
    whole script on every widget interaction and does so on a thread that
    changes; a psycopg connection cached across those reruns is a race that
    surfaces as `InFailedSqlTransaction` on an unrelated panel. The queries here
    are all against pre-aggregated marts and take milliseconds, so the
    connection is not the cost.
    """
    try:
        with connect("analytics_ro", vectors=False) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            columns = [d.name for d in cur.description]
    except UpstreamUnavailable as exc:
        raise WarehouseUnavailable(str(exc)) from exc
    except RuntimeError as exc:
        # Raised by `settings.dsn()` when the role's password is not configured
        # at all — a different failure from the server being down, and it
        # happens *before* any connection is attempted, so `server_reachable()`
        # never gets a chance to report it.
        #
        # Both mean the same thing to a caller: there is no warehouse here. On
        # a fresh clone with no `.env`, or a CI runner that deliberately has
        # none, the unmapped version propagated and turned six dashboard tests
        # from skips into failures.
        if "not set" not in str(exc):
            raise
        raise WarehouseUnavailable(f"not configured: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        if "does not exist" in message:
            raise WarehouseUnavailable("gold has not been built — run `make gold`") from exc
        raise
    return _coerce_decimals(pd.DataFrame(rows, columns=columns))


def _coerce_decimals(frame: pd.DataFrame) -> pd.DataFrame:
    """Turn `decimal.Decimal` columns into floats, at the boundary.

    psycopg maps Postgres `numeric` to `decimal.Decimal`, which is the right
    default — it is what stops money arithmetic drifting — and it is wrong for
    every consumer downstream of here. A column of Decimals lands in pandas as
    dtype `object`, so `.sum()` works, `.nlargest()` raises
    `cannot use method 'nlargest' with this dtype`, and Plotly renders an empty
    axis. Every one of those is a different symptom of the same cause.

    Converted here rather than in each caller, because "remember to cast" is not
    a rule that survives the next panel. The marts are already aggregated in
    SQL, where the exact arithmetic happened; float is the correct type for
    something about to become a pixel.

    Found by `dashboard/screenshots.py`, which is the only automated thing that
    renders these panels — Streamlit writes an exception into the page rather
    than failing the process, so every unit test passed while seven of eight
    tabs were broken.
    """
    import decimal

    for column in frame.columns:
        if frame[column].dtype != object:
            continue
        non_null = frame[column].dropna()
        if not non_null.empty and isinstance(non_null.iloc[0], decimal.Decimal):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


# ---------------------------------------------------------------------------
# freshness
# ---------------------------------------------------------------------------


def watermark() -> Watermark:
    """The cache key, per CONTRACTS.md §7.

    `completed_at IS NOT NULL` is the whole point: a run that is still going has
    a row with a null `completed_at`, so an in-flight load cannot advance the
    watermark and present partial data as fresh.
    """
    frame = _query(
        """
        SELECT
            (SELECT max(completed_at) FROM meta.pipeline_run_log
             WHERE completed_at IS NOT NULL AND status = 'SUCCESS')          AS completed_at,
            (SELECT step FROM meta.pipeline_run_log
             WHERE completed_at IS NOT NULL AND status = 'SUCCESS'
             ORDER BY completed_at DESC LIMIT 1)                             AS step,
            (SELECT count(*) FROM meta.pipeline_run_log
             WHERE completed_at IS NULL AND status = 'RUNNING')::int          AS runs_in_flight
        """
    )
    row = frame.iloc[0]
    return Watermark(
        completed_at=row["completed_at"],
        step=row["step"],
        runs_in_flight=int(row["runs_in_flight"] or 0),
    )


def date_bounds() -> tuple[dt.date, dt.date]:
    """The range the marts actually cover.

    Read from the data rather than from `now()`. The generator writes to a fixed
    anchor, so a date picker defaulting to "the last 30 days" of wall-clock time
    would open on an empty chart the moment the demo is a month old.
    """
    frame = _query("SELECT min(date_day) AS lo, max(date_day) AS hi FROM gold.mart_daily_sales")
    row = frame.iloc[0]
    if pd.isna(row["lo"]):
        raise WarehouseUnavailable("mart_daily_sales is empty — run `make gold`")
    return row["lo"], row["hi"]


def channels() -> list[str]:
    return _query("SELECT DISTINCT channel FROM gold.mart_daily_sales ORDER BY 1")[
        "channel"
    ].tolist()


# ---------------------------------------------------------------------------
# the marts
# ---------------------------------------------------------------------------


def daily_sales(start: dt.date, end: dt.date, channel: str | None = None) -> pd.DataFrame:
    sql = """
        SELECT date_day, channel, orders, revenue_orders, cancelled_orders,
               returned_orders, customers, revenue, discount, units, margin,
               captured_amount, nadd_aov, nadd_return_rate_pct, nadd_revenue_7d_avg
        FROM gold.mart_daily_sales
        WHERE date_day BETWEEN %s AND %s
    """
    params: tuple = (start, end)
    if channel:
        sql += " AND channel = %s"
        params += (channel,)
    return _query(sql + " ORDER BY date_day, channel", params)


def customer_rfm() -> pd.DataFrame:
    return _query(
        """
        SELECT customer_id, loyalty_tier, segment, country, rfm_segment,
               r_score, f_score, m_score, rfm_score,
               recency_days, frequency, monetary, nadd_avg_order_value, is_deleted
        FROM gold.mart_customer_rfm
        """
    )


def cohort_retention(max_periods: int = 12) -> pd.DataFrame:
    return _query(
        """
        SELECT cohort_month, months_since, cohort_size, active_customers,
               nadd_retention_pct
        FROM gold.mart_cohort_retention
        WHERE months_since <= %s
        ORDER BY cohort_month, months_since
        """,
        (max_periods,),
    )


def product_performance(start: dt.date, end: dt.date) -> pd.DataFrame:
    return _query(
        """
        SELECT month_start_date, product_id, product_name, category, subcategory,
               units, revenue, cost, margin, orders, customers,
               nadd_margin_pct, nadd_units_per_order
        FROM gold.mart_product_performance
        WHERE month_start_date BETWEEN %s AND %s
        """,
        (start, end),
    )


def payment_health(start: dt.date, end: dt.date) -> pd.DataFrame:
    return _query(
        """
        SELECT date_day, payment_method, attempts, captures, failures, refunds,
               chargebacks, retry_attempts, captured_amount, refunded_amount,
               failure_reason_counts, top_failure_reason,
               nadd_auth_rate, nadd_chargeback_rate
        FROM gold.mart_payment_health
        WHERE date_day BETWEEN %s AND %s
        ORDER BY date_day
        """,
        (start, end),
    )


def web_funnel(start: dt.date, end: dt.date) -> pd.DataFrame:
    return _query(
        """
        SELECT session_date, channel, device_type, sessions, identified_sessions,
               product_view_sessions, add_to_cart_sessions, checkout_sessions,
               purchase_sessions, nadd_session_conversion_pct
        FROM gold.mart_web_funnel
        WHERE session_date BETWEEN %s AND %s
        """,
        (start, end),
    )


def support_health(start: dt.date, end: dt.date) -> pd.DataFrame:
    return _query(
        """
        SELECT date_day, intent, priority, contact_channel, tickets,
               resolved_tickets, open_tickets, pending_tickets,
               negative_tickets, neutral_tickets, positive_tickets,
               ai_enriched, ai_errors, ai_intent_agreements, ai_sentiment_agreements,
               nadd_avg_resolution_hours, nadd_resolution_rate_pct,
               nadd_negative_pct, nadd_ai_coverage_pct, nadd_ai_intent_accuracy_pct
        FROM gold.mart_support_health
        WHERE date_day BETWEEN %s AND %s
        """,
        (start, end),
    )


def data_quality_summary(limit: int = 40) -> pd.DataFrame:
    """The latest outcome per check, across Pandera, custom SQL and dbt.

    `DISTINCT ON` rather than a window function with a filter: `meta.dq_check_results`
    is append-only, so every check has a row per run, and "what is failing now"
    means the most recent one. Showing the whole history would report a failure
    fixed three runs ago as current.
    """
    return _query(
        """
        SELECT DISTINCT ON (source, check_name)
               source, suite, check_name, target_table, severity, status,
               rows_failed, failure_pct, checked_at, message
        FROM meta.dq_check_results
        ORDER BY source, check_name, checked_at DESC
        LIMIT %s
        """,
        (limit,),
    )


def stream_metrics(hours: int = 48) -> pd.DataFrame:
    """What the streaming path counted. Deliberately from `meta`, not `gold`.

    These are not marts and are not presented as such: they are at-least-once
    counts with no late-arrival handling, and they will disagree with
    `mart_web_funnel` over the same events. Showing both is showing the
    latency/correctness tradeoff rather than asserting there isn't one.
    """
    return _query(
        """
        SELECT window_start, metric, dimension, value, is_closed
        FROM meta.stream_metrics
        WHERE window_start > now() - make_interval(hours => %s)
        ORDER BY window_start
        """,
        (hours,),
    )


def consumer_lag() -> pd.DataFrame:
    return _query(
        """
        SELECT DISTINCT ON (consumer_group, topic, partition)
               consumer_group, topic, partition, current_offset, log_end_offset,
               (log_end_offset - current_offset) AS lag, observed_at
        FROM meta.kafka_consumer_offsets
        ORDER BY consumer_group, topic, partition, observed_at DESC
        """
    )


# ---------------------------------------------------------------------------
# recomputing what must not be summed
# ---------------------------------------------------------------------------


def recompute_aov(frame: pd.DataFrame) -> pd.Series:
    """AOV after aggregating across channels or days.

    `sum(nadd_aov)` is the single most common silent dashboard bug: adding
    yesterday's £84 to today's £91 gives £175, which renders as a chart line and
    is not a number. The ratio has to be recomputed from its components after
    every aggregation, and this is the function that does it once.
    """
    # `to_numeric` first, because psycopg returns DECIMAL columns as
    # `decimal.Decimal` and a frame of those divides into an object column that
    # plotly cannot chart. `.where(!= 0)` rather than `.replace(0, pd.NA)`:
    # dividing by NaN yields NaN, while dividing by pandas' NA yields an NAType
    # that `astype(float)` then refuses.
    revenue = pd.to_numeric(frame["revenue"], errors="coerce")
    orders = pd.to_numeric(frame["revenue_orders"], errors="coerce")
    return (revenue / orders.where(orders != 0)).round(2)


def recompute_rate(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Any percentage, recomputed post-aggregation. Same rule, same reason."""
    top = pd.to_numeric(numerator, errors="coerce")
    bottom = pd.to_numeric(denominator, errors="coerce")
    return (100.0 * top / bottom.where(bottom != 0)).round(2)


def funnel_steps(frame: pd.DataFrame) -> pd.DataFrame:
    """The funnel as ordered steps, with each transition's own conversion.

    Session counts sum across days, channels and devices — a session appears in
    exactly one row — so the *counts* are additive even though every rate
    derived from them is not.
    """
    steps = [
        ("Sessions", "sessions"),
        ("Viewed a product", "product_view_sessions"),
        ("Added to cart", "add_to_cart_sessions"),
        ("Began checkout", "checkout_sessions"),
        ("Purchased", "purchase_sessions"),
    ]
    totals = [(label, int(frame[column].sum())) for label, column in steps]
    result = pd.DataFrame(totals, columns=["step", "sessions"])
    result["pct_of_top"] = (100.0 * result["sessions"] / max(result["sessions"].iloc[0], 1)).round(
        1
    )
    result["pct_of_previous"] = (
        100.0 * result["sessions"] / result["sessions"].shift(1).fillna(result["sessions"].iloc[0])
    ).round(1)
    return result
