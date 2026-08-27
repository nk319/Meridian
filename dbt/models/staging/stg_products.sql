select
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    unit_price,
    unit_cost,

    -- Unit margin, not a rate. A rate here would be non-additive and would end
    -- up summed the first time somebody grouped by category.
    (unit_price - unit_cost) as unit_margin,

    is_active,
    _ingested_at as ingested_at
from {{ source('silver', 'products') }}
