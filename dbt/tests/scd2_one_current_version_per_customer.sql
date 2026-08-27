{#
    Exactly one current version per customer — never zero, never two.

    Two is the overlap bug above, seen from the other end. Zero is subtler and
    is what a mishandled hard delete produces: the last version gets closed and
    no new one opens, so the customer silently disappears from every query that
    joins on `is_current`, including the customer count. `hard_deletes='new_record'`
    is what prevents it — the tombstone version is current, and carries
    `is_deleted` so a live-customer filter can still exclude it.
#}

select
    customer_id,
    count(*) filter (where is_current) as current_versions
from {{ ref('dim_customer') }}
group by customer_id
having count(*) filter (where is_current) <> 1
