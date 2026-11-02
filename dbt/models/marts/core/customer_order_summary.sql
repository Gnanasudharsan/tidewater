{#- Customer grain: lifetime order stats, feeds the repeat-customer-rate metric. -#}
select
    customer_id,
    min(ordered_at) as first_ordered_at,
    max(ordered_at) as last_ordered_at,
    count(*) as order_count,
    sum(case when counts_toward_gmv then order_total else 0 end) as lifetime_gmv,
    case when count(*) > 1 then 1 else 0 end as is_repeat_customer
from {{ ref('fct_orders') }}
group by customer_id
