{#
    View -> cart -> checkout -> purchase, by day, channel and device.
    CONTRACTS.md §8.

    Built on sessions, not events: `int_session_funnel` reduces each session to
    the furthest step it reached, so the counts below are monotonically
    decreasing by construction. A funnel counted from raw events is not — a
    session that adds three items to a cart contributes three `add_to_cart`
    events and one `begin_checkout`, and the chart shows more carts than views.

    Anonymous sessions are included. They are most of the top of a real funnel,
    and excluding them inflates every conversion rate below.
#}

with sessions as (
    select
        s.session_date,
        s.channel,
        s.device_type,
        s.session_id,
        s.customer_id,
        s.reached_product_view,
        s.reached_add_to_cart,
        s.reached_checkout,
        s.reached_purchase
    from {{ ref('int_session_funnel') }} s
)

select
    (to_char(session_date, 'YYYYMMDD'))::int as date_sk,
    session_date,
    channel,
    device_type,

    count(*)                                             as sessions,
    count(*) filter (where customer_id is not null)      as identified_sessions,
    count(*) filter (where reached_product_view)         as product_view_sessions,
    count(*) filter (where reached_add_to_cart)          as add_to_cart_sessions,
    count(*) filter (where reached_checkout)             as checkout_sessions,
    count(*) filter (where reached_purchase)             as purchase_sessions,

    -- Every rate here is sessions-at-step over sessions-at-the-step-before, so
    -- each one answers "what fraction survived this transition" rather than
    -- "what fraction of all traffic", which are different numbers that look
    -- alike on a chart.
    case when count(*) filter (where reached_product_view) > 0
         then round(100.0 * count(*) filter (where reached_add_to_cart)
                          / count(*) filter (where reached_product_view), 2)
    end as nadd_view_to_cart_pct,
    case when count(*) filter (where reached_add_to_cart) > 0
         then round(100.0 * count(*) filter (where reached_checkout)
                          / count(*) filter (where reached_add_to_cart), 2)
    end as nadd_cart_to_checkout_pct,
    case when count(*) filter (where reached_checkout) > 0
         then round(100.0 * count(*) filter (where reached_purchase)
                          / count(*) filter (where reached_checkout), 2)
    end as nadd_checkout_to_purchase_pct,
    round(100.0 * count(*) filter (where reached_purchase) / count(*), 2)
        as nadd_session_conversion_pct

from sessions
group by session_date, channel, device_type
