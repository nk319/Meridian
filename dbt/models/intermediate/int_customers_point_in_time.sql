{{ config(materialized='ephemeral') }}

{#
    `silver.customers` as it stood on a given date.

    Ephemeral on purpose. A view would be compiled with whatever
    `snapshot_as_of` was set at the last `dbt run`, and the snapshot that reads
    it would then silently reconstruct the wrong date; ephemeral means the
    var is compiled into the snapshot's own SQL, so one `dbt snapshot --vars`
    invocation is self-contained and cannot go stale.

    With no `snapshot_as_of` set this is `stg_customers` minus the hard-deleted
    rows — which is exactly what the ordinary daily snapshot should see, so the
    normal path pays nothing for the backfill machinery.

    The rewind rule, per customer and per watched field:

      1. the `new_value` of the latest change at or before the as-of date, else
      2. the `old_value` of the earliest change after it, else
      3. today's value, because the field never changed.

    Rule 2 is the one that is easy to leave out and produces a plausible-looking
    wrong answer: a customer whose only transition is in the future of the
    as-of date has no row matching rule 1, and falling straight through to
    rule 3 would backdate their current tier to before they earned it.
#}

with as_of as (
    select
        {%- if var('snapshot_as_of', none) is not none %}
        '{{ var("snapshot_as_of") }}'::timestamptz as ts
        {%- else %}
        {{ current_timestamp() }} as ts
        {%- endif %}
),

changes as (
    select
        c.customer_id,
        c.field,
        c.changed_at,
        c.old_value,
        c.new_value,
        a.ts as as_of_ts
    from {{ ref('stg_customer_change_log') }} c
    cross join as_of a
),

-- Rule 1: the most recent change we would already have seen.
before_as_of as (
    select distinct on (customer_id, field)
        customer_id,
        field,
        new_value as value
    from changes
    where changed_at <= as_of_ts
    order by customer_id, field, changed_at desc
),

-- Rule 2: the state immediately prior to the first change we have not seen yet.
after_as_of as (
    select distinct on (customer_id, field)
        customer_id,
        field,
        old_value as value
    from changes
    where changed_at > as_of_ts
    order by customer_id, field, changed_at asc
),

resolved as (
    select
        coalesce(b.customer_id, f.customer_id) as customer_id,
        coalesce(b.field, f.field)             as field,
        coalesce(b.value, f.value)             as value
    from before_as_of b
    full outer join after_as_of f
        on b.customer_id = f.customer_id
       and b.field = f.field
),

rewound as (
    select
        s.customer_id,
        s.city,
        s.country,
        s.signup_date,
        coalesce(
            max(case when r.field = 'loyalty_tier' then r.value end),
            s.loyalty_tier
        ) as loyalty_tier,
        coalesce(
            max(case when r.field = 'segment' then r.value end),
            s.segment
        ) as segment,
        -- A hard delete is a change like any other; it just decides whether the
        -- row exists rather than what it says. Stored as text in the change
        -- log, so it is compared as text.
        coalesce(
            max(case when r.field = 'is_deleted' then r.value end),
            case when s.is_deleted then 'true' else 'false' end
        ) as is_deleted_as_of,
        a.ts as as_of_ts
    from {{ ref('stg_customers') }} s
    cross join as_of a
    left join resolved r on r.customer_id = s.customer_id
    group by s.customer_id, s.city, s.country, s.signup_date,
             s.loyalty_tier, s.segment, s.is_deleted, a.ts
)

select
    customer_id,
    city,
    country,
    signup_date,
    loyalty_tier,
    segment
from rewound
-- A customer who had not signed up yet did not exist to be snapshotted, and a
-- deleted one no longer does. Both are absences from this result, which is
-- precisely what `hard_deletes='new_record'` is watching for.
where signup_date <= as_of_ts::date
  and is_deleted_as_of = 'false'
