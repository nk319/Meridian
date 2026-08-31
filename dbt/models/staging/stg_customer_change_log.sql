select
    customer_id,
    changed_at,
    field,
    old_value,
    new_value,
    _ingested_at as ingested_at
from {{ source('silver', 'customer_change_log') }}
