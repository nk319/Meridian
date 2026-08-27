{#
    Revenue, orders and AOV by day and channel. CONTRACTS.md §8.

    Grain is (day, channel) rather than day alone so "which channel is growing"
    is answerable without going back to the fact table. The dashboard sums
    across channels for a total — which works for `revenue` and `orders` and is
    exactly wrong for `nadd_aov`, and that is the point of the prefix.

    Every row of the calendar in range is present, including the ones with no
    orders. A revenue chart built from `group by order_date` has no zeros in it,
    so a dead week renders as a straight line between the days either side of
    it, which reads as "flat" rather than "nothing happened".
#}

with channels as (
    select distinct channel from {{ ref('stg_orders') }}
),

spine as (
    select d.date_sk, d.date_day, d.month_start_date, d.is_weekend, c.channel
    from {{ ref('dim_date') }} d
    cross join channels c
    where d.date_day between
        (select min(order_date) from {{ ref('fact_orders') }})
        and (select max(order_date) from {{ ref('fact_orders') }})
),

daily as (
    select
        date_sk,
        channel,
        count(*)                                    as orders,
        count(*) filter (where is_revenue_recognised) as revenue_orders,
        count(*) filter (where status = 'cancelled') as cancelled_orders,
        count(*) filter (where status = 'returned')  as returned_orders,
        count(distinct customer_id)                 as customers,
        sum(revenue_amount)                         as revenue,
        sum(discount_amount)                        as discount,
        sum(shipping_amount)                        as shipping,
        sum(tax_amount)                             as tax,
        sum(unit_count)                             as units,
        sum(margin_amount)                          as margin,
        sum(captured_amount)                        as captured_amount
    from {{ ref('fact_orders') }}
    group by date_sk, channel
)

select
    s.date_sk,
    s.date_day,
    s.month_start_date,
    s.is_weekend,
    s.channel,

    coalesce(d.orders, 0)           as orders,
    coalesce(d.revenue_orders, 0)   as revenue_orders,
    coalesce(d.cancelled_orders, 0) as cancelled_orders,
    coalesce(d.returned_orders, 0)  as returned_orders,
    coalesce(d.customers, 0)        as customers,

    coalesce(d.revenue, 0)          as revenue,
    coalesce(d.discount, 0)         as discount,
    coalesce(d.shipping, 0)         as shipping,
    coalesce(d.tax, 0)              as tax,
    coalesce(d.units, 0)            as units,
    coalesce(d.margin, 0)           as margin,
    coalesce(d.captured_amount, 0)  as captured_amount,

    -- A ratio. Summing it across channels or days gives a number with no
    -- meaning, so it says so in its name. Recompute from revenue/orders after
    -- any aggregation instead.
    case when coalesce(d.revenue_orders, 0) > 0
         then round(d.revenue / d.revenue_orders, 2)
    end as nadd_aov,

    case when coalesce(d.orders, 0) > 0
         then round(100.0 * d.returned_orders / d.orders, 2)
    end as nadd_return_rate_pct,

    -- Trailing seven days within the channel. Ordered by date and framed by
    -- rows rather than by range, which is only safe because the spine above
    -- guarantees no missing days.
    round(avg(coalesce(d.revenue, 0)) over (
        partition by s.channel order by s.date_day
        rows between 6 preceding and current row
    ), 2) as nadd_revenue_7d_avg

from spine s
left join daily d on d.date_sk = s.date_sk and d.channel = s.channel
