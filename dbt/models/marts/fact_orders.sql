{#
    Order-header grain: one row per order. CONTRACTS.md §8.

    The half of the grain split that makes revenue, order count and AOV
    answerable without `count(distinct order_id)` in every query. `total_amount`
    lives here and only here — putting it on the line fact would multiply it by
    the number of lines the first time anyone summed it.

    The customer join is the point of having an SCD2 dimension at all: it
    resolves to the customer *version that was current when the order was
    placed*, so "revenue by loyalty tier" reports the tier the customer actually
    held at the time rather than the tier they hold today. Joining to
    `is_current` instead is the mistake this whole dimension exists to prevent,
    and it is invisible — the numbers still add up, they are just answers to a
    different question.
#}

select
    {{ surrogate_key(['o.order_id']) }} as order_sk,

    -- Degenerate dimension: the natural key, kept on the fact because support
    -- and finance both look orders up by it and a dimension holding nothing but
    -- an id would be a join for no information.
    o.order_id,

    c.customer_sk,
    o.customer_id,
    (to_char(o.order_date, 'YYYYMMDD'))::int as date_sk,

    o.order_ts,
    o.order_date,
    o.status,
    o.channel,
    o.device_type,

    o.gross_amount,
    o.discount_amount,
    o.shipping_amount,
    o.tax_amount,
    o.total_amount,

    -- Revenue with the business rule applied, so a mart never has to remember
    -- it. Cancelled and returned orders keep their `total_amount` — they
    -- happened — and contribute zero here.
    case when o.is_revenue_recognised then o.total_amount else 0 end as revenue_amount,

    coalesce(r.line_count, 0)   as line_count,
    coalesce(r.unit_count, 0)   as unit_count,
    coalesce(r.distinct_product_count, 0) as distinct_product_count,
    r.margin_amount_total       as margin_amount,

    coalesce(p.payment_attempts, 0)  as payment_attempts,
    coalesce(p.captured_amount, 0)   as captured_amount,
    coalesce(p.refunded_amount, 0)   as refunded_amount,
    coalesce(p.is_paid, false)       as is_paid,
    p.last_failure_reason,

    o.is_revenue_recognised,
    o.is_delivered

from {{ ref('stg_orders') }} o

-- As-of join. `valid_from`/`valid_to` are closed at both ends in dim_customer
-- precisely so this is a plain BETWEEN with no coalesce and no null handling —
-- and so it cannot silently drop an order placed before the snapshot's history
-- begins.
left join {{ ref('dim_customer') }} c
       on c.customer_id = o.customer_id
      and o.order_ts >= c.valid_from
      and o.order_ts <  c.valid_to

left join {{ ref('int_order_line_rollup') }} r on r.order_id = o.order_id
left join {{ ref('int_order_payments') }}    p on p.order_id = o.order_id
