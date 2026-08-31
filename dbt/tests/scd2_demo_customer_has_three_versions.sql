{#
    The designated SCD2 demo customer has exactly three versions, in tier order.

    CONTRACTS.md §8 requires this test by name, and the reason is that the other
    two SCD2 tests above pass perfectly against a dimension containing no
    history at all: one version per customer never overlaps, and is always
    exactly one current version. They are necessary and they are not sufficient.

    This one fails if the snapshot degenerates to current-state-only — which is
    what happens if the backfill is skipped, if the change log stops being
    ingested, or if the `check` strategy stops watching `loyalty_tier`. It is
    the test that notices the SCD2 machinery has quietly stopped working.

    Coupled to seed values on purpose. `meridian.seed.config` emits C000042 with
    three backdated transitions specifically so there is something here to
    assert, and a test that avoided naming them could not tell "three versions"
    from "three copies of the same version".
#}

with actual as (
    select
        count(*)                                        as versions,
        count(*) filter (where is_current)               as current_versions,
        string_agg(loyalty_tier, ',' order by version)   as tier_path,
        bool_and(is_deleted is false)                    as never_deleted
    from {{ ref('dim_customer') }}
    where customer_id = 'C000042'
)

select *
from actual
where versions <> 3
   or current_versions <> 1
   or tier_path <> 'silver,gold,platinum'
   or not never_deleted
