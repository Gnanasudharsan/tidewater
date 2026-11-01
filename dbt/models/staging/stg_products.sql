select
    product_id,
    sku,
    name as product_name,
    category,
    unit_price,
    is_active,
    cast(created_at as {{ dbt.type_timestamp() }}) as created_at,
    cast(updated_at as {{ dbt.type_timestamp() }}) as updated_at
from {{ source('silver', 'products') }}
where not is_deleted
