select
    order_item_id,
    order_id,
    line_number,
    product_id,
    quantity,
    unit_price,
    line_amount,
    _ingested_at as ingested_at
from {{ source('silver', 'order_items') }}
