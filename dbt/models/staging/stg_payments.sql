select
    payment_id,
    order_id,
    amount,
    method as payment_method,
    status as payment_status,
    amount < 0 as is_refund,
    cast(created_at as {{ dbt.type_timestamp() }}) as paid_at,
    _wh_loaded_at
from {{ source('silver', 'payments') }}
where not is_deleted
