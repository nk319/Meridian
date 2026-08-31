{#
    SCD2 history for customers, per CONTRACTS.md §8.

    `check` rather than `timestamp`, on `loyalty_tier` and `segment` only.
    The timestamp strategy would key off `updated_at`, which the source system
    touches on any column — so a city correction would close the current row and
    open a new one, and `dim_customer` would grow versions that record nothing a
    dimension is supposed to track. `check` says outright which changes are
    slowly-changing-dimension changes and which are just edits.

    `hard_deletes='new_record'` rather than the default `ignore`. A customer
    deleted in the source simply stops appearing here; under `ignore` their last
    version stays `is_current` forever and the dimension keeps asserting they
    are a live platinum customer. `new_record` appends a tombstone version
    instead, so the fact that they left is itself a dated event — which is what
    a warehouse is for. `invalidate_hard_deletes` is the older spelling of a
    weaker version of this and is deprecated in dbt 1.9+.
#}

{% snapshot customers_snapshot %}

{{
    config(
        target_schema='gold_int',
        unique_key='customer_id',
        strategy='check',
        check_cols=['loyalty_tier', 'segment'],
        hard_deletes='new_record',
    )
}}

select * from {{ ref('int_customers_point_in_time') }}

{% endsnapshot %}
