{# ========================================================
   Macro: get_columns_for_table
   Purpose: Introspect column names from a given schema/table
   ======================================================== #}
{% macro get_columns_for_table(schema_name, table_name) %}
    {% set query %}
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{{ schema_name }}'
          AND TABLE_NAME = '{{ table_name }}'
        ORDER BY ORDINAL_POSITION
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set cols = results.columns[0].values() %}
    {% else %}
        {% set cols = [] %}
    {% endif %}

    {{ return(cols) }}
{% endmacro %}


{# ========================================================
   Macro: build_staging
   Purpose: Build staging tables dynamically (stg_<table>)
   ======================================================== #}
{% macro build_staging(schema_name, table_name) %}
    {{ config(
        materialized='table',
        schema=schema_name,
        alias='stg_' ~ table_name
    ) }}
    
    select * from {{ schema_name }}.{{ table_name }}
{% endmacro %}


{# ========================================================
   Macro: build_scd2
   Purpose: Build SCD2 tables dynamically (dim_<table>_scd2)
   Uses MERGE for true SCD2 behaviour
   ======================================================== #}
{% macro build_scd2(schema_name, table_name, business_key) %}

    {# Grab all the columns from the base table #}
    {% set cols = get_columns_for_table(schema_name, table_name) %}
    {% set tracked_cols = cols | reject("equalto", business_key) | list %}

    {{ config(
        materialized='incremental',
        schema=schema_name,
        alias='dim_' ~ table_name ~ '_scd2',
        unique_key=business_key,
        on_schema_change='sync_all_columns'
    ) }}

    {# First run: full load - insert all rows into dimension #}
    {% if not is_incremental() %}
        select
            {{ business_key }} as business_key,
            {% for col in tracked_cols %}
                {{ col }}{% if not loop.last %}, {% endif %}
            {% endfor %},
            sysdatetime() as valid_from,
            cast(null as datetime) as valid_to,
            1 as is_current
        from {{ schema_name }}.{{ table_name }}

    {% else %}

        {# Incremental / subsequent runs: update changed rows and insert new versions #}
        merge {{ this }} as target
        using (
            select
                {{ business_key }} as business_key,
                {% for col in tracked_cols %}
                    {{ col }}{% if not loop.last %}, {% endif %}
                {% endfor %}
            from {{ schema_name }}.{{ table_name }}
        ) as source
        on target.business_key = source.business_key and target.is_current = 1

        when matched and (
            {% for col in tracked_cols %}
                target.{{ col }} <> source.{{ col }}
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        )
        then update set 
            target.valid_to = sysdatetime(),
            target.is_current = 0

        when not matched by target then
            insert (business_key, {% for col in tracked_cols %}{{ col }},{% endfor %} valid_from, valid_to, is_current)
            values (source.business_key, {% for col in tracked_cols %}source.{{ col }},{% endfor %} sysdatetime(), null, 1);

    {% endif %}

{% endmacro %}