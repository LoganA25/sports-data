{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' -%}
        {{ node.fqn[-2] | trim }}
    {%- else -%}
        {{ target.schema }}_{{ node.fqn[-2] | trim }}
    {%- endif -%}
{%- endmacro %}