{#
    Order-line grain: one row per line. CONTRACTS.md §8.

    The half of the split that makes `dim_product` joinable and "top products by
    revenue" a group-by rather than an unnest. `line_amount` is additive at this
    grain; `total_amount` deliberately is not here at all, because a header
    measure on a line fact is a fan-out waiting for its first SUM.

    Both facts carry `customer_sk` and `date_sk` from the same order, so the two
    can be filtered consistently without joining them to each other.
#}

select
    {{ surrogate_key(['l.order_item_id']) }} as order_item_sk,

    l.order_item_id,
    l.order_id,
    {{ surrogate_key(['l.order_id']) }} as order_sk,
    l.line_number,

    -- Null when the line points at a product missing from the catalogue export.
    -- The DQ suite tracks that at a measured 1.46% as a WARN; the fact keeps
    -- the row and its revenue either way, which is the difference between a
    -- known gap in one dimension and vanished money.
    d.product_sk,
    l.product_id,

    c.customer_sk,
    o.customer_id,
    (to_char(o.order_date, 'YYYYMMDD'))::int as date_sk,
    o.order_ts,
    o.order_date,
    o.status,
    o.channel,

    l.quantity,
    l.unit_price,
    l.line_amount,
    l.unit_cost,
    l.cost_amount,
    l.margin_amount,

    case when o.is_revenue_recognised then l.line_amount else 0 end as revenue_amount,
    o.is_revenue_recognised

from {{ ref('int_order_lines') }} l
join {{ ref('stg_orders') }} o on o.order_id = l.order_id

left join {{ ref('dim_product') }} d on d.product_id = l.product_id
left join {{ ref('dim_customer') }} c
       on c.customer_id = o.customer_id
      and o.order_ts >= c.valid_from
      and o.order_ts <  c.valid_to
