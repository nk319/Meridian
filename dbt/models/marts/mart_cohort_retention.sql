{#
    Retention by acquisition cohort. CONTRACTS.md §8.

    Cohort is the month of a customer's *first order*, not their signup month.
    Signup cohorts measure how good marketing was at collecting registrations;
    order cohorts measure whether the people who bought once came back, which is
    the question retention is asked to answer.

    Grain is (cohort_month, months_since) — one cell of the heatmap per row. The
    triangle is deliberately not squared off: a cohort acquired last month
    cannot have a month-6 number, and emitting a zero there would draw a cliff
    on the chart that is an artefact of the calendar rather than of churn.
#}

with first_orders as (
    select
        customer_id,
        date_trunc('month', min(order_date))::date as cohort_month
    from {{ ref('fact_orders') }}
    where is_revenue_recognised
    group by customer_id
),

cohort_sizes as (
    select cohort_month, count(*) as cohort_size
    from first_orders
    group by cohort_month
),

activity as (
    select
        f.cohort_month,
        date_trunc('month', o.order_date)::date as activity_month,
        o.customer_id,
        o.revenue_amount
    from {{ ref('fact_orders') }} o
    join first_orders f on f.customer_id = o.customer_id
    where o.is_revenue_recognised
),

cells as (
    select
        cohort_month,
        activity_month,
        -- Whole months between the two, which is what the heatmap's axis is.
        -- Subtracting dates and dividing by 30 drifts by a day every other
        -- month and eventually mislabels a column.
        (extract(year  from age(activity_month, cohort_month)) * 12
         + extract(month from age(activity_month, cohort_month)))::int as months_since,
        count(distinct customer_id) as active_customers,
        count(*)                    as orders,
        sum(revenue_amount)         as revenue
    from activity
    group by cohort_month, activity_month
)

select
    c.cohort_month,
    c.months_since,
    c.activity_month,
    s.cohort_size,
    c.active_customers,
    c.orders,
    c.revenue,

    -- The heatmap's value. Non-additive twice over: across cohorts because the
    -- denominators differ, and across periods because the same customer appears
    -- in several.
    round(100.0 * c.active_customers / s.cohort_size, 2) as nadd_retention_pct,
    round(c.revenue / s.cohort_size, 2)                  as nadd_revenue_per_cohort_member

from cells c
join cohort_sizes s on s.cohort_month = c.cohort_month
