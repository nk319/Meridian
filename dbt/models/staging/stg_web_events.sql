select
    event_id,
    session_id,
    customer_id,
    event_ts,
    event_ts::date as event_date,
    event_type,
    product_id,
    order_id,
    channel,
    device_type,
    _ingested_at as ingested_at
from {{ source('silver', 'web_events') }}
