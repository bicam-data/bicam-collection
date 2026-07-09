{% macro unnest_array(json_expr, array_path, alias) -%}
jsonb_array_elements(
  coalesce(
    {{ json_expr }} #> {{ bicam_json_path_literal(array_path) }},
    '[]'::jsonb
  )
) as {{ alias }}(value)
{%- endmacro %}
