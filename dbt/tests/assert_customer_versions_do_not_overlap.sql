-- SCD2 integrity: a customer's versions tile time without gaps or overlaps.
with ordered as (
    select
        customer_id,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by customer_id order by valid_from) as next_from
    from {{ ref('dim_customer') }}
)

select * from ordered
where next_from is not null and (valid_to is null or valid_to <> next_from)
