{#
    One row per clickstream event. The largest fact here by an order of
    magnitude, and the only one where `customer_sk` is legitimately null:
    anonymous browsing is most of a real funnel, and dropping it would make
    every conversion rate look far better than it is.
#}

select
    {{ surrogate_key(['e.event_id']) }} as event_sk,

    e.event_id,
    e.session_id,

    c.customer_sk,
    e.customer_id,
    d.product_sk,
    e.product_id,
    e.order_id,
    (to_char(e.event_date, 'YYYYMMDD'))::int as date_sk,

    e.event_ts,
    e.event_date,
    e.event_type,
    e.channel,
    e.device_type

from {{ ref('stg_web_events') }} e
left join {{ ref('dim_product') }} d on d.product_id = e.product_id
left join {{ ref('dim_customer') }} c
       on c.customer_id = e.customer_id
      and e.event_ts >= c.valid_from
      and e.event_ts <  c.valid_to
