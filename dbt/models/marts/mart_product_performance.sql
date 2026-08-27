{#
    Product economics by month. CONTRACTS.md §8.

    Built on `fact_order_items`, which is the whole reason the grain is split:
    at header grain this mart cannot exist, because an order does not have a
    product.

    Monthly rather than all-time so the dashboard can scope "top products" to a
    period. Every measure except the two prefixed ones is additive across both
    months and products, so a period total is a plain SUM.
#}

with monthly as (
    select
        d.month_start_date,
        i.product_id,
        count(*)                          as line_count,
        count(distinct i.order_id)        as orders,
        count(distinct i.customer_id)     as customers,
        sum(i.quantity)                   as units,
        sum(i.revenue_amount)             as revenue,
        sum(i.cost_amount)                as cost,
        sum(i.margin_amount)              as margin,
        sum(i.line_amount) filter (where i.status = 'returned') as returned_amount
    from {{ ref('fact_order_items') }} i
    join {{ ref('dim_date') }} d on d.date_sk = i.date_sk
    group by d.month_start_date, i.product_id
)

select
    m.month_start_date,

    -- Null for the ~1.5% of lines whose product is absent from the catalogue
    -- export. Carried rather than filtered: the revenue is real and belongs in
    -- the period total, and a mart that quietly drops it will disagree with
    -- mart_daily_sales by an amount nobody can account for.
    p.product_sk,
    m.product_id,
    coalesce(p.product_name, '(unknown product)') as product_name,
    coalesce(p.category, '(uncategorised)')       as category,
    coalesce(p.subcategory, '(uncategorised)')    as subcategory,
    p.is_active,

    m.line_count,
    m.orders,
    m.customers,
    m.units,
    m.revenue,
    m.cost,
    m.margin,
    coalesce(m.returned_amount, 0) as returned_amount,

    case when m.revenue > 0
         then round(100.0 * m.margin / m.revenue, 2)
    end as nadd_margin_pct,
    case when m.orders > 0
         then round(m.units::numeric / m.orders, 2)
    end as nadd_units_per_order

from monthly m
left join {{ ref('dim_product') }} p on p.product_id = m.product_id
