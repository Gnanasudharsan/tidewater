select
    customer_id,
    email,
    full_name,
    phone,
    address_line,
    city,
    state,
    postal_code,
    country,
    cast(created_at as {{ dbt.type_timestamp() }}) as created_at,
    cast(updated_at as {{ dbt.type_timestamp() }}) as updated_at,
    _source_ts as cdc_source_ts
from {{ source('silver', 'customers') }}
where not is_deleted
