{{ config(
    materialized='incremental',
    unique_key='payment_id',
    post_hook="{{ delete_soft_deleted('payment_id', 'payments') }}",
) }}

select
    payment_id,
    order_id,
    cast(paid_at as date) as payment_date,
    paid_at,
    payment_method,
    payment_status,
    amount,
    is_refund,
    _wh_loaded_at as _source_loaded_at
from {{ ref('stg_payments') }}
{% if is_incremental() %}
where _wh_loaded_at > {{ incremental_since() }}
{% endif %}
