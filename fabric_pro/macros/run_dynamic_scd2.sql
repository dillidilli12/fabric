{% macro run_dynamic_scd2(scd2_sources) %}
  {% for tbl in scd2_sources %}
    {{ log("Building staging + scd2 for " ~ tbl['schema'] ~ "." ~ tbl['table'], info=true) }}

    -- Create / refresh staging
    {% set staging_sql = build_staging(tbl['schema'], tbl['table']) %}
    {% do run_query(staging_sql) %}

    -- Create / update SCD2
    {% set scd2_sql = build_scd2(tbl['schema'], tbl['table'], tbl['business_key']) %}
    {% do run_query(scd2_sql) %}

  {% endfor %}
{% endmacro %}