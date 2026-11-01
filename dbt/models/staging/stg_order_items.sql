select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    cast(quantity * unit_price as {{ dbt.type_numeric() }}) as line_total,
    cast(created_at as {{ dbt.type_timestamp() }}) as created_at
from {{ source('silver', 'order_items') }}
where not is_deleted
