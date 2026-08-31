{#
    Line-level totals rolled up to the order header.

    `fact_orders` carries the item and unit counts so that "average basket size"
    is answerable at header grain without touching the line fact. This is the
    one thing that makes the grain split cheap: each fact answers its own
    questions, and neither has to be joined to the other for the common ones.
#}

select
    order_id,
    count(*)                     as line_count,
    sum(quantity)                as unit_count,
    count(distinct product_id)   as distinct_product_count,
    sum(line_amount)             as line_amount_total,
    sum(cost_amount)             as cost_amount_total,
    sum(margin_amount)           as margin_amount_total
from {{ ref('int_order_lines') }}
group by order_id
