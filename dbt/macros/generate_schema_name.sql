-- Override the default schema-name behaviour so each layer gets its own schema
-- (staging → stock_analytics_staging, marts → stock_analytics_marts, etc.)
-- instead of the default which just uses the target schema.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
