{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {# Exact layer names are reserved for this controlled local target.
       Dev/test/future production keep dbt's default namespace prefixing.
       Each environment must use its own target schema/catalog. #}
    {%- if target.name == 'local_acceptance'
          and node is not none and node.package_name == 'local_lakehouse'
          and custom_schema_name is not none
          and custom_schema_name | trim in ['silver', 'gold'] -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ dbt.default__generate_schema_name(custom_schema_name, node) }}
    {%- endif -%}
{%- endmacro %}
