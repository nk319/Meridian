{#
    Every fact table still has the grain it claims.

    The as-of joins to `dim_customer` are the risk: a `between` against a type-2
    dimension multiplies rows the instant two versions overlap, and the result
    is a warehouse where every number is too big by a factor that varies per
    customer. `scd2_no_overlapping_versions` tests the cause; this tests the
    effect, at the point where it would do the damage.

    Stated as a union so one failing fact names itself in the output rather than
    turning up as an unattributed count.
#}

select 'fact_orders' as model, count(*) - count(distinct order_id) as extra_rows
from {{ ref('fact_orders') }}
having count(*) <> count(distinct order_id)

union all
select 'fact_order_items', count(*) - count(distinct order_item_id)
from {{ ref('fact_order_items') }}
having count(*) <> count(distinct order_item_id)

union all
select 'fact_payments', count(*) - count(distinct payment_id)
from {{ ref('fact_payments') }}
having count(*) <> count(distinct payment_id)

union all
select 'fact_web_events', count(*) - count(distinct event_id)
from {{ ref('fact_web_events') }}
having count(*) <> count(distinct event_id)

union all
select 'fact_support_tickets', count(*) - count(distinct ticket_id)
from {{ ref('fact_support_tickets') }}
having count(*) <> count(distinct ticket_id)
