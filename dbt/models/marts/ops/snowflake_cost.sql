{#- Snowflake-only: credits consumed by the Tidewater warehouse per hour, against the
    rows the loader delivered in the same hour -> credits per million rows.
    ACCOUNT_USAGE lags by up to ~3 hours. -#}
{{ config(enabled=(target.type == 'snowflake'), materialized='table') }}

with credits as (
    select
        date_trunc('hour', start_time) as hour,
        sum(credits_used) as credits_used,
        sum(credits_used_compute) as credits_compute
    from snowflake.account_usage.warehouse_metering_history
    where warehouse_name = 'TIDEWATER_WH'
    group by 1
),

rows_loaded as (
    select date_trunc('hour', loaded_at) as hour, sum(rows_loaded) as rows_loaded
    from {{ source('silver', '_load_log') }}
    group by 1
)

select
    c.hour,
    c.credits_used,
    c.credits_compute,
    coalesce(r.rows_loaded, 0) as rows_loaded,
    c.credits_used / nullif(r.rows_loaded, 0) * 1000000 as credits_per_million_rows
from credits as c
left join rows_loaded as r on c.hour = r.hour
