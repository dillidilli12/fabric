{# 
Driver model: 
Loops through multiple schemas/tables dynamically 
and builds both staging + SCD2 dimension tables 
#}

{% for tbl in var('scd2_sources', []) %}

    {{ build_staging(tbl['schema'], tbl['table']) }}

    {{ build_scd2(tbl['schema'], tbl['table'], tbl['business_key']) }}

{% endfor %}