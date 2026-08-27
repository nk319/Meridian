{#
    No customer has two versions valid at the same instant.

    The defining property of a type-2 dimension, and the one that decides
    whether an as-of join fans out. Two overlapping versions turn every
    `fact_orders` row for that customer into two rows, revenue doubles for
    exactly the customers who changed tier, and nothing errors.

    Windows are half-open — `[valid_from, valid_to)` — which is why the
    predicate is strict on both sides. Using `<=` would flag the ordinary case
    where one version ends at the exact instant the next begins, and a test that
    fails on correct data gets disabled.
#}

select
    a.customer_id,
    a.version    as version_a,
    b.version    as version_b,
    a.valid_from as a_from,
    a.valid_to   as a_to,
    b.valid_from as b_from,
    b.valid_to   as b_to
from {{ ref('dim_customer') }} a
join {{ ref('dim_customer') }} b
  on a.customer_id = b.customer_id
 and a.version < b.version
where a.valid_from < b.valid_to
  and b.valid_from < a.valid_to
