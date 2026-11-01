select
    order_id,
    customer_id,
    status as order_status,
    order_total,
    currency,
    cast(created_at as {{ dbt.type_timestamp() }}) as ordered_at,
    cast(updated_at as {{ dbt.type_timestamp() }}) as updated_at
from {{ source('silver', 'orders') }}
where not is_deleted
