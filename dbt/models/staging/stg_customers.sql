{#
    Rename and type, nothing else.

    Staging is deliberately thin: one model per source table, no joins, no
    filters that drop rows. Anything that decides what a row *means* belongs in
    intermediate or marts, where it is visible. The one job staging does have is
    to be the only place in the project that knows the physical source names, so
    a column rename in silver is a one-file change here rather than a search
    across forty models.
#}

select
    customer_id,
    city,
    country,
    signup_date,
    loyalty_tier,
    segment,
    is_deleted,
    updated_at,
    _ingested_at as ingested_at
from {{ source('silver', 'customers') }}
