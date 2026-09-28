{#
    Schema naming:  dev  -> <target.schema>_<custom>  e.g. DEV_AKSHAY_STAGING (per-developer, isolated)
                    prod -> <custom>                  e.g. STAGING            (clean names)
    dbt's built-in default would give CORE_STAGING in prod.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
