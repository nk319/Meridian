{#
    Authorisation rate and failure mix, by day and method. CONTRACTS.md §8.

    Grain is the *attempt*, aggregated — which is why this reads from
    `fact_payments` rather than from `fact_orders.is_paid`. An order that
    succeeded on the third try is a paid order and two declines, and a mart
    built at order grain can only ever see the first fact.

    The failure breakdown is a jsonb object rather than one column per reason.
    Reasons come from the payment provider and change without warning; a column
    per reason means a schema migration every time one is added, and a row per
    reason would put this mart at a different grain from its own auth rate.
#}

with attempts as (
    select
        p.date_sk,
        d.date_day,
        d.month_start_date,
        p.payment_method,
        count(*)                                         as attempts,
        count(*) filter (where p.is_captured)             as captures,
        count(*) filter (where p.status = 'authorized')   as authorizations,
        count(*) filter (where p.status = 'failed')       as failures,
        count(*) filter (where p.status = 'refunded')     as refunds,
        count(*) filter (where p.status = 'chargeback')   as chargebacks,
        count(distinct p.order_id)                        as orders_attempted,
        count(distinct p.order_id) filter (where p.is_captured) as orders_captured,
        sum(p.captured_amount)                            as captured_amount,
        sum(p.amount) filter (where p.status = 'refunded') as refunded_amount,
        count(*) filter (where p.attempt_number > 1)      as retry_attempts
    from {{ ref('fact_payments') }} p
    join {{ ref('dim_date') }} d on d.date_sk = p.date_sk
    group by p.date_sk, d.date_day, d.month_start_date, p.payment_method
),

reasons as (
    select
        date_sk,
        payment_method,
        jsonb_object_agg(failure_reason, n) as failure_reason_counts,
        (array_agg(failure_reason order by n desc))[1] as top_failure_reason
    from (
        select date_sk, payment_method, failure_reason, count(*) as n
        from {{ ref('fact_payments') }}
        where failure_reason is not null
        group by date_sk, payment_method, failure_reason
    ) x
    group by date_sk, payment_method
)

select
    a.date_sk,
    a.date_day,
    a.month_start_date,
    a.payment_method,

    a.attempts,
    a.captures,
    a.authorizations,
    a.failures,
    a.refunds,
    a.chargebacks,
    a.retry_attempts,
    a.orders_attempted,
    a.orders_captured,
    a.captured_amount,
    coalesce(a.refunded_amount, 0) as refunded_amount,

    coalesce(r.failure_reason_counts, '{}'::jsonb) as failure_reason_counts,
    r.top_failure_reason,

    -- Captures over attempts. The named example in CONTRACTS.md §8 of a measure
    -- that must never be summed: adding yesterday's 92% to today's 91% is 183%.
    case when a.attempts > 0
         then round(100.0 * a.captures / a.attempts, 2)
    end as nadd_auth_rate,
    case when a.orders_attempted > 0
         then round(100.0 * a.orders_captured / a.orders_attempted, 2)
    end as nadd_order_capture_rate,
    case when a.attempts > 0
         then round(100.0 * a.chargebacks / a.attempts, 2)
    end as nadd_chargeback_rate

from attempts a
left join reasons r on r.date_sk = a.date_sk and r.payment_method = a.payment_method
