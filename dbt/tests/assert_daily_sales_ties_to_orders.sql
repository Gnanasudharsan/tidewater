-- The incremental daily rollup must equal a full recomputation from fct_orders.
with recomputed as (
    select
        order_date,
        count(*) as orders,
        sum(case when counts_toward_gmv then order_total else 0 end) as gmv
    from {{ ref('fct_orders') }}
    group by order_date
)

select r.order_date, r.orders, d.orders as daily_orders, r.gmv, d.gmv as daily_gmv
from recomputed as r
left join {{ ref('fct_daily_sales') }} as d on r.order_date = d.order_date
where d.order_date is null or r.orders <> d.orders or r.gmv <> d.gmv
