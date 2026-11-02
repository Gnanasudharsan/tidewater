{#- Type 1 product dimension: current attributes only (price history lives on order lines). -#}
select
    product_id,
    sku,
    product_name,
    category,
    unit_price as current_unit_price,
    is_active,
    created_at,
    updated_at
from {{ ref('stg_products') }}
