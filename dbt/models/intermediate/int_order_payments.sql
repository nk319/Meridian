{#
    Payment attempts rolled up to the order.

    An order can be attempted several times — a decline followed by a retry on a
    different card is the common shape — so "did this order get paid" is not a
    property of any single payment row. Computed once here because
    `fact_orders`, `mart_payment_health` and `mart_daily_sales` all need the
    same answer, and three copies of a `sum(case when status = 'captured' ...)`
    is three chances to disagree about whether an authorisation counts.
#}

select
    order_id,

    count(*)                                  as payment_attempts,
    count(*) filter (where is_captured)        as captured_attempts,
    count(*) filter (where status = 'failed')  as failed_attempts,
    count(*) filter (where status = 'refunded') as refunded_attempts,
    count(*) filter (where status = 'chargeback') as chargeback_attempts,

    -- Only captures move money. Summing every attempt would double-count a
    -- retry, and summing authorisations would count a hold that may expire.
    coalesce(sum(amount) filter (where is_captured), 0)   as captured_amount,
    coalesce(sum(amount) filter (where status = 'refunded'), 0) as refunded_amount,

    bool_or(is_captured)                       as is_paid,
    min(processed_ts) filter (where is_captured) as first_captured_ts,
    max(processed_ts)                          as last_attempt_ts,

    -- The reason on the last failure, which is the one a human investigating
    -- an unpaid order actually wants. Earlier failures are in fact_payments.
    (array_agg(failure_reason order by processed_ts desc)
        filter (where failure_reason is not null))[1] as last_failure_reason

from {{ ref('stg_payments') }}
group by order_id
