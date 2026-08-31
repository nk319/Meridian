{#
    One row per browsing session, with the furthest funnel step it reached.

    A funnel is not a count of events. Counting `add_to_cart` events counts a
    session that added four items four times, and counting them per step
    produces a "funnel" where a later step can exceed an earlier one. The unit
    of a funnel is the session, and the measure is whether that session ever
    reached a step.

    Steps are numbered so "furthest reached" is a max() rather than five
    correlated subqueries. `search` and `page_view` are deliberately outside the
    ladder: they are entry behaviour, not progress toward a purchase.
#}

with stepped as (
    select
        session_id,
        customer_id,
        channel,
        device_type,
        event_ts,
        event_date,
        case event_type
            when 'product_view'   then 1
            when 'add_to_cart'    then 2
            when 'begin_checkout' then 3
            when 'purchase'       then 4
            else 0
        end as funnel_step
    from {{ ref('stg_web_events') }}
)

select
    session_id,

    -- A session can span an anonymous prefix and an identified suffix once the
    -- visitor logs in. max() keeps the identity if one ever appeared, which is
    -- what makes the session attributable at all.
    max(customer_id)   as customer_id,
    -- First touch, not last: the channel that brought the session is the one
    -- worth crediting, and it is the first event that carries it.
    (array_agg(channel     order by event_ts))[1] as channel,
    (array_agg(device_type order by event_ts))[1] as device_type,

    min(event_ts)      as session_start_ts,
    max(event_ts)      as session_end_ts,
    min(event_date)    as session_date,
    count(*)           as event_count,

    max(funnel_step)   as max_funnel_step,
    bool_or(funnel_step >= 1) as reached_product_view,
    bool_or(funnel_step >= 2) as reached_add_to_cart,
    bool_or(funnel_step >= 3) as reached_checkout,
    bool_or(funnel_step >= 4) as reached_purchase

from stepped
group by session_id
