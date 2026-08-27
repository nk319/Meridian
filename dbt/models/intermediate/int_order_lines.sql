{#
    Order lines with the product economics attached.

    The join to products happens once, here, rather than in `fact_order_items`
    and again in `mart_product_performance`. Cost is a product attribute at the
    time of sale; this project's catalogue has no price history, so it is the
    current cost — which is stated rather than hidden, because a margin computed
    against today's cost for a two-year-old order is an approximation and the
    reader should know it is one.
#}

select
    i.order_item_id,
    i.order_id,
    i.line_number,
    i.product_id,
    i.quantity,
    i.unit_price,
    i.line_amount,

    p.unit_cost,
    (i.quantity * p.unit_cost)                 as cost_amount,
    (i.line_amount - i.quantity * p.unit_cost) as margin_amount,

    p.category,
    p.subcategory

from {{ ref('stg_order_items') }} i
-- Left, not inner. The seed injects order lines pointing at products that are
-- not in the catalogue export — a real and common condition when two systems
-- are exported at different times — and the DQ suite tracks it as a WARN at a
-- measured 1.46% rather than pretending it cannot happen. An inner join here
-- would delete those lines and their revenue from the warehouse without
-- anything failing.
left join {{ ref('stg_products') }} p on p.product_id = i.product_id
