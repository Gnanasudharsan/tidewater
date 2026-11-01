{#- Use the configured schema as-is (staging, marts) instead of dbt's default
    "<target_schema>_<custom>" prefixing, so names match on Postgres and Snowflake. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
