with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-01-01' as date)",
        end_date="cast('2028-01-01' as date)"
    ) }}
)

select
    cast(date_day as date) as date_day,
    cast({{ dbt.date_trunc('week', 'date_day') }} as date) as week_start,
    cast({{ dbt.date_trunc('month', 'date_day') }} as date) as month_start,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(dow from date_day) as day_of_week,
    extract(dow from date_day) in (0, 6) as is_weekend
from spine
