{#
    What the snapshot considers "now".

    dbt's default is the wall clock, which is right for every ordinary run and
    wrong for exactly one thing: replaying history into a snapshot that was
    added after the data already existed.

    That is not a contrived situation. `silver.customers` holds current state
    only, and `silver.customer_change_log` holds the transitions that produced
    it. A snapshot run once against current state produces one version per
    customer, `is_current` is true everywhere, `dbt_valid_to` is null
    everywhere, and every SCD2 test passes against a result set that contains
    no history at all — the tautology CONTRACTS.md §8 warns about.

    Overriding this macro is the documented extension point for it. The
    backfill (`make dbt-snapshot-backfill`) walks the change log's transition
    timestamps, and at each one reconstructs the customer table as it stood on
    that date and snapshots it. The effect is that `dbt_valid_from` carries the
    date the tier actually changed, rather than the date somebody happened to
    run the backfill.

    Unset — every normal run — this is `now()` and dbt behaves exactly as it
    does without the override.
#}
{% macro snapshot_get_time() -%}
    {%- if var('snapshot_as_of', none) is not none -%}
        {{ "'" ~ var('snapshot_as_of') ~ "'::timestamptz" }}
    {%- else -%}
        {{ current_timestamp() }}
    {%- endif -%}
{%- endmacro %}
