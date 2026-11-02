{{ config(
    materialized='incremental',
    unique_key='order_id',
    post_hook="{{ delete_soft_deleted('order_id', 'orders') }}",
) }}

{#- Order grain. An order is re-built when the order, any of its lines, or any of
    its payments was loaded since the last run (minus the lookback). -#}
with orders as (
    select
        *,
        row_number() over (
            partition by customer_id order by ordered_at, order_id
        ) as customer_order_number
    from {{ ref('stg_orders') }}
),

items as (
    select
        order_id,
        count(*) as item_count,
        sum(quantity) as units,
        max(_wh_loaded_at) as loaded_at
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(case when payment_status = 'captured' then amount else 0 end) as captured_amount,
        sum(case when is_refund then -amount else 0 end) as refunded_amount,
        max(_wh_loaded_at) as loaded_at
    from {{ ref('stg_payments') }}
    group by order_id
),

{% if is_incremental() %}
changed as (
    select order_id from {{ ref('stg_orders') }}
    where _wh_loaded_at > {{ incremental_since() }}
    union
    select order_id from {{ ref('stg_order_items') }}
    where _wh_loaded_at > {{ incremental_since() }}
    union
    select order_id from {{ ref('stg_payments') }}
    where _wh_loaded_at > {{ incremental_since() }}
),
{% endif %}

final as (
    select
        o.order_id,
        c.customer_sk,
        o.customer_id,
        cast(o.ordered_at as date) as order_date,
        o.ordered_at,
        o.order_status,
        o.currency,
        o.order_total,
        coalesce(i.item_count, 0) as item_count,
        coalesce(i.units, 0) as units,
        coalesce(p.captured_amount, 0) as captured_amount,
        coalesce(p.refunded_amount, 0) as refunded_amount,
        o.order_status = 'refunded' as is_refunded,
        o.order_status <> 'cancelled' as counts_toward_gmv,
        o.customer_order_number,
        o.customer_order_number > 1 as is_repeat_order,
        greatest(
            o._wh_loaded_at,
            coalesce(i.loaded_at, o._wh_loaded_at),
            coalesce(p.loaded_at, o._wh_loaded_at)
        ) as _source_loaded_at
    from orders as o
    left join items as i on o.order_id = i.order_id
    left join payments as p on o.order_id = p.order_id
    -- SCD2 as-of join: the customer version that was valid when the order was placed.
    left join {{ ref('dim_customer') }} as c
        on o.customer_id = c.customer_id
        and o.ordered_at >= c.valid_from
        and (c.valid_to is null or o.ordered_at < c.valid_to)
)

select * from final
{% if is_incremental() %}
where order_id in (select order_id from changed)
{% endif %}
