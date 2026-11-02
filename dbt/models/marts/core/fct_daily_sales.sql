{{ config(materialized='incremental', unique_key='order_date') }}

{#- Day grain, rebuilt only for days that had an order change or deletion since
    the last run. -#}
{% if is_incremental() %}
with touched_days as (
    select order_date from {{ ref('fct_orders') }}
    where _source_loaded_at > {{ incremental_since() }}
    union
    select cast(created_at as date) from {{ source('silver', 'orders') }}
    where is_deleted and _wh_loaded_at > {{ incremental_since() }}
)
{% endif %}

select
    order_date,
    count(*) as orders,
    sum(case when counts_toward_gmv then order_total else 0 end) as gmv,
    sum(case when counts_toward_gmv then order_total else 0 end)
        / nullif(sum(case when counts_toward_gmv then 1 else 0 end), 0) as aov,
    sum(units) as units,
    sum(case when is_refunded then 1 else 0 end) as refunded_orders,
    sum(refunded_amount) as refunded_amount,
    count(distinct customer_id) as customers,
    sum(case when is_repeat_order then 1 else 0 end) as repeat_orders,
    max(_source_loaded_at) as _source_loaded_at
from {{ ref('fct_orders') }}
{% if is_incremental() %}
where order_date in (select order_date from touched_days)
{% endif %}
group by order_date
