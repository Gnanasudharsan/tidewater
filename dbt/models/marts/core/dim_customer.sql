{#- One row per customer version (SCD Type 2). Facts join on customer_sk as of the
    event time, so an order keeps the address the customer had when they ordered. -#}
with versions as (
    select
        *,
        row_number() over (partition by customer_id order by dbt_valid_from) as version_number
    from {{ ref('snap_customers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_sk,
    customer_id,
    version_number,
    email,
    full_name,
    phone,
    address_line,
    city,
    state,
    postal_code,
    country,
    created_at,
    -- The first version also covers everything before the first snapshot ran.
    case
        when version_number = 1 then cast('{{ var("scd_start") }}' as {{ dbt.type_timestamp() }})
        else dbt_valid_from
    end as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from versions
