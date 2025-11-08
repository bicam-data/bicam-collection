{% macro generate_schema_name(custom_schema_name, node) -%}
  {#
    Override dbt's default schema naming to avoid prefixing target.schema.
    If a custom schema is set on the model, use it as-is; otherwise use target.schema.
  #}
  {% if custom_schema_name is not none and custom_schema_name|length > 0 %}
    {{ return(custom_schema_name) }}
  {% else %}
    {{ return(target.schema) }}
  {% endif %}
{%- endmacro %}

