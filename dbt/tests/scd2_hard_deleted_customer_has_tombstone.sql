{#
    The hard-deleted customer's final version is a tombstone.

    The other half of what CONTRACTS.md §8 asks `hard_deletes='new_record'` to
    demonstrate. Under dbt's default (`ignore`) a customer deleted in the source
    keeps their last version marked current forever, and the dimension goes on
    asserting they are a live customer — a failure with no error message
    attached to it.

    Asserted as a shape rather than a count: the customer must have a version
    that is current, flagged deleted, and preceded by at least one that is not.
#}

with versions as (
    select is_current, is_deleted, version
    from {{ ref('dim_customer') }}
    where customer_id = 'C000117'
),

shape as (
    select
        count(*)                                                as total,
        count(*) filter (where is_current and is_deleted)         as tombstones,
        count(*) filter (where not is_current and not is_deleted) as live_history
    from versions
)

select * from shape
where tombstones <> 1 or live_history < 1 or total < 2
