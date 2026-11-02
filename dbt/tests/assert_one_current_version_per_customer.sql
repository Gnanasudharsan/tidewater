-- SCD2 integrity: no customer has more than one current version, and every live
-- customer has exactly one. Customers deleted at the source are closed out by the
-- snapshot (invalidate_hard_deletes), so they correctly have none.
with current_counts as (
    select customer_id, sum(case when is_current then 1 else 0 end) as current_versions
    from {{ ref('dim_customer') }}
    group by customer_id
)

select c.customer_id, c.current_versions, s.customer_id is not null as is_live
from current_counts as c
left join {{ ref('stg_customers') }} as s on c.customer_id = s.customer_id
where c.current_versions > 1
    or (s.customer_id is not null and c.current_versions <> 1)
