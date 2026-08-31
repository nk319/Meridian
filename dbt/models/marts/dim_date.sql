{#
    One row per calendar day.

    Generated rather than derived from the facts. A date dimension built from
    `select distinct order_date` has holes on days nobody ordered, and a holed
    date dimension turns "revenue by day" into a chart that silently omits the
    zeros — which are the days you most want to see.

    The range is anchored on `var('anchor_date')`, the same anchor the seed
    generator uses, so the calendar covers the data rather than drifting past it
    as the wall clock moves.
#}

with bounds as (
    select
        ('{{ var("anchor_date") }}'::date - interval '3 years')::date as start_date,
        ('{{ var("anchor_date") }}'::date + interval '1 year')::date  as end_date
),

days as (
    select generate_series(start_date, end_date, interval '1 day')::date as date_day
    from bounds
)

select
    -- The one surrogate key in the warehouse that is not a hash. A date's
    -- natural key is already unique, immutable, dense and orderable, so
    -- yyyymmdd carries all four for free and makes a fact table partitionable
    -- and range-scannable on it. Hashing would throw that away to buy
    -- consistency with dimensions whose natural keys have none of it.
    (to_char(date_day, 'YYYYMMDD'))::int as date_sk,
    date_day,

    extract(year    from date_day)::int as year,
    extract(quarter from date_day)::int as quarter,
    extract(month   from date_day)::int as month,
    extract(day     from date_day)::int as day_of_month,
    extract(week    from date_day)::int as week_of_year,
    -- ISO: Monday = 1, Sunday = 7. Postgres offers both `dow` (Sunday = 0) and
    -- `isodow`; picking one and saying which avoids the off-by-one that shows up
    -- as a weekend chart shifted by a day.
    extract(isodow  from date_day)::int as day_of_week,

    trim(to_char(date_day, 'Month')) as month_name,
    trim(to_char(date_day, 'Day'))   as day_name,

    date_trunc('month',   date_day)::date as month_start_date,
    date_trunc('quarter', date_day)::date as quarter_start_date,
    date_trunc('year',    date_day)::date as year_start_date,
    (date_trunc('month', date_day) + interval '1 month - 1 day')::date as month_end_date,

    (extract(isodow from date_day) >= 6) as is_weekend,

    -- Relative to the generator's anchor, not to now(). A dashboard that says
    -- "last 30 days" against a fixed seed needs a fixed reference, or the demo
    -- goes blank the moment the data ages out of the window.
    (date_day - '{{ var("anchor_date") }}'::date) as days_from_anchor,
    (date_day > '{{ var("anchor_date") }}'::date) as is_future

from days
