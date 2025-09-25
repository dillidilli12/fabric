{# ========================================================
   Macro: get_columns_for_table
   Purpose: Introspect source columns + data types from Fabric Warehouse
   ======================================================== #}
{% macro get_columns_for_table(schema_name, table_name) %}
    {% set query %}
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{{ schema_name }}'
          AND TABLE_NAME = '{{ table_name }}'
        ORDER BY ORDINAL_POSITION
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set columns = [] %}
        {% for row in results %}
            {% set col_name = row[0] %}
            {% set data_type = row[1]|lower %}
            {% set char_len  = row[2] %}
            {% set num_prec  = row[3] %}
            {% set num_scale = row[4] %}
            
            {# Build a proper type definition #}
            {% if data_type in ['varchar','nvarchar','char','nchar'] and char_len is not none %}
                {% set type_def = data_type ~ '(' ~ (char_len if char_len > 0 else 'max') ~ ')' %}
            {% elif data_type in ['decimal','numeric'] and num_prec is not none %}
                {% set type_def = data_type ~ '(' ~ num_prec ~ ',' ~ num_scale ~ ')' %}
            {% elif data_type == 'datetime2' %}
                {% set type_def = 'datetime2(6)' %}   {# enforce scale to avoid Fabric error #}
            {% elif data_type in ['datetime','smalldatetime','date'] %}
                {% set type_def = data_type %}
            {% else %}
                {% set type_def = data_type %}
            {% endif %}

            {% do columns.append({'name': col_name, 'type': type_def}) %}
        {% endfor %}
    {% else %}
        {% set columns = [] %}
    {% endif %}

    {{ return(columns) }}
{% endmacro %}


{# ========================================================
   Macro: build_staging
   Purpose: Create physical staging tables stg_<table>
   ======================================================== #}
{% macro build_staging(schema_name, table_name) %}
    {% set sql %}
        if object_id('{{ schema_name }}.stg_{{ table_name }}','U') is not null
            drop table {{ schema_name }}.stg_{{ table_name }};

        select * 
        into {{ schema_name }}.stg_{{ table_name }}
        from {{ schema_name }}.{{ table_name }};
    {% endset %}

    {{ return(sql) }}
{% endmacro %}


{# ========================================================
   Macro: build_scd2
   Purpose: Create / maintain dynamic SCD2 table dim_<table>_scd2
   - Preserves source datatypes
   - Inserts new rows, expires old rows with valid_to/is_current
   ======================================================== #}
{% macro build_scd2(schema_name, table_name, business_key) %}

    {# Get column names + data types #}
    {% set cols_info = get_columns_for_table(schema_name, table_name) %}
    {% set non_keys = [] %}
    {% for col in cols_info %}
        {% if col['name'] != business_key %}
            {% do non_keys.append(col) %}
        {% endif %}
    {% endfor %}

    {% set sql %}
        -- Create dimension table if not exists
        if object_id('{{ schema_name }}.dim_{{ table_name }}_scd2','U') is null
        begin
            create table {{ schema_name }}.dim_{{ table_name }}_scd2 (
                business_key {{ (cols_info | selectattr('name','equalto',business_key) | list)[0]['type'] }},
                {% for col in non_keys %}
                    {{ col['name'] }} {{ col['type'] }},
                {% endfor %}
                valid_from datetime2(6),
                valid_to datetime2(6),
                is_current bit
            )
        end;

        -- Merge incoming source rows with existing dimension
        merge {{ schema_name }}.dim_{{ table_name }}_scd2 as target
        using (
            select
                {{ business_key }} as business_key,
                {% for col in non_keys %}
                    {{ col['name'] }}{% if not loop.last %}, {% endif %}
                {% endfor %}
            from {{ schema_name }}.{{ table_name }}
        ) as source
        on target.business_key = source.business_key and target.is_current = 1

        -- 1: If data changed for business_key, expire old row
        when matched and (
            {% for col in non_keys %}
                isnull(cast(target.{{ col['name'] }} as {{ col['type'] }}),'') <> isnull(cast(source.{{ col['name'] }} as {{ col['type'] }}),''){% if not loop.last %} or {% endif %}
            {% endfor %}
        )
        then update set 
            target.valid_to = sysdatetime(),
            target.is_current = 0

        -- 2: If new row (key not found), insert
        when not matched by target then
            insert (business_key, {% for col in non_keys %}{{ col['name'] }},{% endfor %} valid_from, valid_to, is_current)
            values (source.business_key, {% for col in non_keys %}source.{{ col['name'] }},{% endfor %} sysdatetime(), null, 1);
    {% endset %}

    {{ return(sql) }}
{% endmacro %}