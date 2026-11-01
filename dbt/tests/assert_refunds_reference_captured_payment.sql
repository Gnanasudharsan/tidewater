-- Every refund row must offset a captured payment of the same order and amount.
select r.payment_id
from {{ ref('stg_payments') }} as r
left join {{ ref('stg_payments') }} as p
    on p.order_id = r.order_id
    and p.payment_status = 'captured'
    and p.amount = -r.amount
where r.is_refund and p.payment_id is null
