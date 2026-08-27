"""What the star schema has to be true of, asserted from outside dbt.

dbt already tests itself, and this file deliberately does not repeat those
assertions. It covers three things `dbt test` structurally cannot:

  * **That the models built at all.** A green `dbt test` on a project whose
    models were never run is 87 passes against yesterday's tables — or, on a
    fresh warehouse, an error rather than a failure. `make test` should notice.

  * **What the dashboard will actually do with the output.** `nadd_` is a
    naming convention, and a convention nothing checks is a comment. The test
    below reads the information schema and fails if a ratio-shaped column is
    missing the prefix, which is the only way the rule survives the next mart.

  * **That the grants are what CONTRACTS.md §1 says.** dbt runs as `dbt_runner`
    and therefore cannot discover that `analytics_ro` — the role the dashboard
    and every AI handler connect as — has been left without SELECT on a new
    mart. The failure mode is a dashboard that works for whoever built it.

Everything here connects as `analytics_ro`, for that last reason: these are
assertions about what a consumer can see.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.db

MARTS = (
    "mart_daily_sales",
    "mart_customer_rfm",
    "mart_cohort_retention",
    "mart_product_performance",
    "mart_payment_health",
    "mart_web_funnel",
    "mart_support_health",
)

DIMS_AND_FACTS = (
    "dim_date",
    "dim_customer",
    "dim_product",
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_web_events",
    "fact_support_tickets",
)


@pytest.fixture(scope="module")
def gold(db):
    """Skip rather than fail when the transform layer has not been built."""
    with db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'gold' AND table_name = 'fact_orders'"
        )
        (n,) = cur.fetchone()
    if not n:
        pytest.skip("gold not built — run `make gold`")
    return db


def scalar(conn, sql, *params):
    with conn.cursor() as cur:
        cur.execute(sql, params or None)
        row = cur.fetchone()
    return row[0] if row else None


def test_every_contracted_table_exists_and_is_readable(gold):
    """CONTRACTS.md §8 names fifteen relations. All fifteen, by name.

    Reading each one rather than checking the catalogue: a table that exists and
    that `analytics_ro` cannot select from is, to the dashboard, a table that
    does not exist.
    """
    for name in DIMS_AND_FACTS + MARTS:
        assert scalar(gold, f"SELECT count(*) FROM gold.{name}") is not None, name


def test_no_contracted_table_is_empty(gold):
    """An empty mart passes every dbt test it has.

    Uniqueness, not-null and accepted-values are all vacuously true on zero
    rows, so a mart whose join silently matched nothing is green in dbt and
    blank on the dashboard. This is the assertion that separates those.
    """
    empty = [
        name
        for name in DIMS_AND_FACTS + MARTS
        if scalar(gold, f"SELECT count(*) FROM gold.{name}") == 0
    ]
    assert not empty, f"built but empty: {empty}"


def test_the_two_order_grains_agree_on_merchandise_value(gold):
    """The whole justification for splitting the grain.

    Two fact tables built from different sources, each joined independently to a
    type-2 dimension, either of which can fan out or lose rows without the other
    noticing. If they disagree, one of them is wrong and the dashboard is
    reporting whichever number the query happened to reach.

    Compared on `gross_amount`, not `total_amount`: discount, shipping and tax
    are header-level and have no line to belong to.
    """
    difference = scalar(
        gold,
        """
        SELECT abs(
            (SELECT sum(gross_amount) FROM gold.fact_orders)
          - (SELECT sum(line_amount)  FROM gold.fact_order_items)
        )
        """,
    )
    assert difference is not None and difference < 1, f"grains differ by {difference}"


def test_revenue_recognition_is_applied_consistently(gold):
    """`revenue_amount` is `total_amount` with cancelled and returned zeroed.

    Stated once in staging so no mart has to remember it. If the rule drifts,
    this is where it shows up rather than as two dashboard tiles that disagree.
    """
    mismatched = scalar(
        gold,
        """
        SELECT count(*) FROM gold.fact_orders
        WHERE (status IN ('cancelled', 'returned') AND revenue_amount <> 0)
           OR (status NOT IN ('cancelled', 'returned')
               AND revenue_amount <> total_amount)
        """,
    )
    assert mismatched == 0


def test_the_funnel_never_widens(gold):
    """Each step of a funnel must be a subset of the one before it.

    Counted per session in `int_session_funnel` precisely so this holds by
    construction — an event-counted funnel does not, because one session can
    add four items to a cart. Asserted anyway: "by construction" is a claim
    about code that somebody will edit.
    """
    violations = scalar(
        gold,
        """
        SELECT count(*) FROM gold.mart_web_funnel
        WHERE add_to_cart_sessions > product_view_sessions
           OR checkout_sessions    > add_to_cart_sessions
           OR purchase_sessions    > checkout_sessions
        """,
    )
    assert violations == 0


def test_the_daily_sales_calendar_has_no_holes(gold):
    """A gap-free spine is what stops a dead week rendering as a flat line."""
    gaps = scalar(
        gold,
        """
        WITH per_channel AS (
            SELECT channel,
                   count(DISTINCT date_day)                     AS days_present,
                   max(date_day) - min(date_day) + 1            AS days_expected
            FROM gold.mart_daily_sales
            GROUP BY channel
        )
        SELECT count(*) FROM per_channel WHERE days_present <> days_expected
        """,
    )
    assert gaps == 0


def test_non_additive_measures_all_carry_the_prefix(gold):
    """CONTRACTS.md §8's additivity rule, enforced rather than described.

    The heuristic is deliberately shape-based: a column whose name says it is a
    rate, a percentage, an average or a median is non-additive whatever it
    measures. It cannot catch every case — a ratio called `efficiency` slips
    through — but it catches the ones people actually write, and a rule with a
    test is a rule.

    `days_from_anchor` and similar are excluded by only matching the suffixes
    and stems that denote a computed ratio.
    """
    with gold.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'gold'
              AND column_name NOT LIKE 'nadd\\_%'
              AND (column_name LIKE '%\\_rate'
                OR column_name LIKE '%\\_pct'
                OR column_name LIKE '%\\_ratio'
                OR column_name LIKE 'avg\\_%'
                OR column_name LIKE '%\\_avg'
                OR column_name LIKE 'median\\_%'
                OR column_name LIKE '%\\_aov')
            ORDER BY table_name, column_name
            """
        )
        offenders = cur.fetchall()
    assert not offenders, "non-additive measures without the nadd_ prefix: " + ", ".join(
        f"{t}.{c}" for t, c in offenders
    )


def test_every_nadd_column_really_is_a_ratio(gold):
    """The prefix must not become decoration.

    A prefix applied to a summable column is worse than no prefix: it teaches
    the reader that the convention is noise, and the next genuine ratio gets
    summed. Checked by type — every non-additive measure here is numeric, and a
    text or timestamp column carrying the prefix is a mistake.
    """
    with gold.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'gold'
              AND column_name LIKE 'nadd\\_%'
              AND data_type NOT IN ('numeric', 'double precision', 'real', 'integer', 'bigint')
            """
        )
        wrong = cur.fetchall()
    assert not wrong, f"nadd_ on non-numeric columns: {wrong}"


def test_scd2_history_is_real_and_not_a_single_snapshot(gold):
    """The dimension has more rows than it has customers.

    `dbt test` covers overlap, currency and the demo customer. What it cannot
    tell you from inside the project is whether the *warehouse* somebody is
    looking at has history in it — a fresh clone that skipped
    `make dbt-snapshot-backfill` passes every dbt SCD2 test except the demo one
    and has a dimension with no history at all.
    """
    versions = scalar(gold, "SELECT count(*) FROM gold.dim_customer")
    customers = scalar(gold, "SELECT count(DISTINCT customer_id) FROM gold.dim_customer")
    assert versions > customers, "dim_customer has one version per customer — no history"


def test_the_hard_deleted_customer_is_current_and_flagged(gold):
    """`hard_deletes='new_record'`, seen from the consumer's side.

    Under dbt's default the deleted customer's last version stays current
    forever and the dimension goes on asserting they are live. The tombstone is
    what makes "they left" a dated fact — and `is_deleted` is what stops a
    live-customer filter from being `WHERE is_current`.
    """
    row = scalar(
        gold,
        """
        SELECT count(*) FROM gold.dim_customer
        WHERE is_current AND is_deleted
        """,
    )
    assert row >= 1, "no tombstone rows — hard deletes are being ignored"


def test_facts_join_to_a_customer_version_that_existed_at_the_time(gold):
    """Every order resolves to exactly one customer version.

    Zero would mean the as-of join dropped it — which happens the moment the
    earliest version stops being open-ended, because the snapshot's history
    starts months after the orders do. That failure is silent: an inner join to
    a dimension is exactly as quiet as a filter, and six months of revenue
    leaves the warehouse without an error.
    """
    unmatched = scalar(gold, "SELECT count(*) FROM gold.fact_orders WHERE customer_sk IS NULL")
    assert unmatched == 0


def test_analytics_ro_can_read_every_mart(gold):
    """The dashboard's role, on the tables the dashboard reads.

    dbt runs as `dbt_runner` and cannot discover this: `ALTER DEFAULT
    PRIVILEGES` only covers objects created after it was set, so a mart added
    before the grant — or created by a different role — is invisible to the
    consumer and perfectly visible to whoever built it.
    """
    for mart in MARTS:
        assert scalar(gold, f"SELECT count(*) FROM gold.{mart}") is not None, mart


def test_analytics_ro_still_cannot_read_silver_or_secure(gold):
    """The transform layer did not widen the blast radius.

    CONTRACTS.md §1 gives `analytics_ro` gold, rag and meta — not silver, not
    oltp, and above all not secure. Phase 4 added a schema and a set of grants;
    this is the check that it added them where it meant to.
    """
    import psycopg

    for relation in ("silver.customers", "secure.customer_pii"):
        with pytest.raises(psycopg.errors.InsufficientPrivilege), gold.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {relation} LIMIT 1")
        # The failed statement poisons the transaction; without this the next
        # iteration raises InFailedSqlTransaction and the test passes for the
        # wrong reason.
        gold.rollback()
