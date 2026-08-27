select
    order_id,
    customer_id,
    order_ts,
    order_date,
    status,
    channel,
    device_type,
    gross_amount,
    discount_amount,
    shipping_amount,
    tax_amount,
    total_amount,

    -- Derived here rather than repeated in five marts. Cancelled and returned
    -- orders are real orders that happened; they are just not revenue, and
    -- every revenue measure downstream has to agree on that. Naming the rule
    -- once is the difference between "revenue" meaning one thing and meaning
    -- whatever the last person to write a mart assumed.
    (status not in ('cancelled', 'returned')) as is_revenue_recognised,
    (status = 'delivered')                    as is_delivered,

    _ingested_at as ingested_at
from {{ source('silver', 'orders') }}
