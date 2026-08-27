select
    payment_id,
    order_id,
    attempt_number,
    payment_method,
    status,
    amount,
    processed_ts,
    processed_ts::date as processed_date,
    failure_reason,

    -- The two states that mean money moved, named once. `authorized` is not
    -- captured: an authorisation that never captures is a hold that expires,
    -- and counting it as revenue is the classic payments reporting bug.
    (status = 'captured')                       as is_captured,
    (status in ('captured', 'authorized'))      as is_successful_attempt,

    _ingested_at as ingested_at
from {{ source('silver', 'payments') }}
