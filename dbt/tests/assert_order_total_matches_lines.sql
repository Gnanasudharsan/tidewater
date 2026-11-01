-- An order's total must equal the sum of its lines: catches partial loads where
-- an order arrived but some of its lines did not (or vice versa).
select o.order_id, o.order_total, sum(i.line_total) as lines_total
from {{ ref('stg_orders') }} as o
inner join {{ ref('stg_order_items') }} as i on o.order_id = i.order_id
group by o.order_id, o.order_total
having o.order_total <> sum(i.line_total)
