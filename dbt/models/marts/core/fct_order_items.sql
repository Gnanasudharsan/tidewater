{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    post_hook="{{ delete_soft_deleted('order_item_id', 'order_items') }}",
) }}

{#- Order-line grain. unit_price is the price paid, not today's catalog price. -#}
select
    i.order_item_id,
    i.order_id,
    o.customer_id,
    i.product_id,
    cast(o.ordered_at as date) as order_date,
    i.quantity,
    i.unit_price,
    i.line_total,
    greatest(i._wh_loaded_at, o._wh_loaded_at) as _source_loaded_at
from {{ ref('stg_order_items') }} as i
inner join {{ ref('stg_orders') }} as o on i.order_id = o.order_id
{% if is_incremental() %}
where greatest(i._wh_loaded_at, o._wh_loaded_at) > {{ incremental_since() }}
{% endif %}
