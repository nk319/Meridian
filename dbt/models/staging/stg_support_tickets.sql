select
    ticket_id,
    customer_id,
    order_id,
    created_ts,
    created_ts::date as created_date,
    resolved_ts,
    status,
    channel as contact_channel,   -- not the marketing channel of the same name
    subject,
    intent,
    priority,
    sentiment,

    -- Null while a ticket is open, which is the honest representation: a
    -- resolution time of zero would drag every average down and look like the
    -- support team got faster.
    case
        when resolved_ts is not null
        then extract(epoch from (resolved_ts - created_ts)) / 3600.0
    end as resolution_hours,

    _ingested_at as ingested_at
from {{ source('silver', 'support_tickets') }}
