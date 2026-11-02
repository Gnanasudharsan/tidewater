{#- SCD Type 2 history of customer attributes (address changes, renames).
    Rows that disappear from staging (soft-deleted at the source) are closed out. -#}
{% snapshot snap_customers %}
{{
    config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True,
    )
}}
select
    customer_id, email, full_name, phone, address_line, city, state, postal_code, country,
    created_at, updated_at
from {{ ref('stg_customers') }}
{% endsnapshot %}
