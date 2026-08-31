{#
    One row per payment *attempt*, not per paid order.

    Keeping the attempt grain is what makes authorisation rate measurable at
    all: an order that succeeded on the second try is one success and one
    failure, and a fact table at order grain can only record the outcome, not
    the cost of getting there.
#}

select
    {{ surrogate_key(['p.payment_id']) }} as payment_sk,

    p.payment_id,
    p.order_id,
    {{ surrogate_key(['p.order_id']) }} as order_sk,

    c.customer_sk,
    o.customer_id,
    (to_char(p.processed_date, 'YYYYMMDD'))::int as date_sk,

    p.processed_ts,
    p.processed_date,
    p.attempt_number,
    p.payment_method,
    p.status,
    p.failure_reason,

    p.amount,
    -- Amount, gated on the outcome, so a mart can SUM it without restating the
    -- rule. `authorized` is excluded: a hold that never captures is not money.
    case when p.is_captured then p.amount else 0 end as captured_amount,

    p.is_captured,
    p.is_successful_attempt

from {{ ref('stg_payments') }} p
left join {{ ref('stg_orders') }} o on o.order_id = p.order_id
-- Attributed to the customer version current when the payment was processed,
-- not when the order was placed. They are usually the same version and
-- occasionally are not, and the payment is its own event.
left join {{ ref('dim_customer') }} c
       on c.customer_id = o.customer_id
      and p.processed_ts >= c.valid_from
      and p.processed_ts <  c.valid_to
