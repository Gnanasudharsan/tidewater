{#- Required by MetricFlow: one row per day. -#}
select date_day from {{ ref('dim_date') }}
