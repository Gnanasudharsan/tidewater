{#- High-water mark for an incremental fact, minus the lookback window
    (see var fact_lookback_minutes). Returns a scalar subquery. -#}
{% macro incremental_since(column='_source_loaded_at') -%}
    (select {{ dbt.dateadd('minute', -1 * var('fact_lookback_minutes'), 'max(' ~ column ~ ')') }}
     from {{ this }})
{%- endmacro %}

{#- Post-hook: remove fact rows whose source row was soft-deleted. Incremental
    merges only upsert, so without this a deleted order would live on forever. -#}
{% macro delete_soft_deleted(key, source_table) -%}
    delete from {{ this }}
    where {{ key }} in (
        select {{ key }} from {{ source('silver', source_table) }} where is_deleted
    )
{%- endmacro %}
