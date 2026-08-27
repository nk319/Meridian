{#
    One row per product. Type 1: overwrite, no history.

    Not an oversight. `silver.products` is a catalogue export with no change
    log, so there is no history to keep — and SCD2 over a source that only ever
    shows current state produces the same one-version-per-key tautology
    CONTRACTS.md §8 warns about on customers. The difference between the two
    dimensions is that the customer source has a change log and this one does
    not.
#}

select
    {{ surrogate_key(['product_id']) }} as product_sk,

    product_id,
    sku,
    product_name,
    category,
    subcategory,

    unit_price,
    unit_cost,
    unit_margin,

    -- Additive nowhere: a margin percentage summed across products is
    -- meaningless, so it carries the prefix even on a dimension, where the
    -- temptation to aggregate is smaller but not zero.
    case
        when unit_price > 0 then round(100.0 * unit_margin / unit_price, 2)
    end as nadd_unit_margin_pct,

    is_active
from {{ ref('stg_products') }}
