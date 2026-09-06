{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
      The polished BICAM product is a stable PostgreSQL interface, not a
      target-schema suffix.  dbt's default implementation would render the
      requested ``bicam`` schema as ``public_bicam`` when the development
      target schema is ``public``.  Keep the product schema exact while
      retaining dbt's normal isolation behavior for every other custom
      schema used by developers or CI.
    #}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif custom_schema_name | trim | lower == 'bicam' -%}
        bicam
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
