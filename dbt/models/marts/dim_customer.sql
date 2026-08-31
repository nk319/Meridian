{#
    SCD2 customer dimension, one row per customer *version*.

    Reads the dbt snapshot and does three things to it: names the columns the
    way CONTRACTS.md §8 freezes them, numbers the versions, and closes the
    validity windows at both ends.

    Both ends matter, and the lower one is the subtle half.

    A snapshot only knows what it has observed. History here starts at the first
    row in `silver.customer_change_log` (2025-02-07 in the default seed) while
    orders start in 2024-08-20 — so a straight `order_ts between valid_from and
    valid_to` join would drop roughly the first six months of revenue on the
    floor, silently, because an inner join to a dimension is exactly as quiet as
    a filter. Opening the earliest version backwards to -infinity says the
    honest thing instead: this is the earliest state we know about, and we are
    attributing everything before our first observation to it.

    Symmetrically, the open version's `valid_to` is `infinity` rather than null.
    A null there is correct and costs a `coalesce` in every single as-of join,
    which is a coalesce somebody eventually forgets.
#}

with versions as (
    select
        customer_id,
        city,
        country,
        signup_date,
        loyalty_tier,
        segment,
        dbt_valid_from,
        dbt_valid_to,
        -- dbt writes this as text, not boolean.
        (dbt_is_deleted = 'True') as is_deleted,
        row_number() over (
            partition by customer_id order by dbt_valid_from
        ) as version
    from {{ ref('customers_snapshot') }}
)

select
    {{ surrogate_key(['customer_id', 'version']) }} as customer_sk,

    customer_id,
    loyalty_tier,
    segment,
    city,
    country,
    signup_date,

    case when version = 1
         then '-infinity'::timestamptz
         else dbt_valid_from
    end as valid_from,
    coalesce(dbt_valid_to, 'infinity'::timestamptz) as valid_to,

    (dbt_valid_to is null) as is_current,
    version,

    -- The tombstone `hard_deletes='new_record'` appends. It is the current
    -- version of a customer who no longer exists, which is a different thing
    -- from no current version at all — and the reason `is_current` alone is not
    -- a filter for "live customers".
    is_deleted

from versions
