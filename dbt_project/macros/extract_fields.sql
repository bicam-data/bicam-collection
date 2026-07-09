{% macro bicam_json_path_literal(path) -%}
  '{{ "{" ~ (path.split(".") | join(",")) ~ "}" }}'
{%- endmacro %}

{% macro bicam_typed_json_expr(json_expr, path, data_type) -%}
  {%- if data_type == "jsonb" -%}
    {{ json_expr }} #> {{ bicam_json_path_literal(path) }}
  {%- else -%}
    {%- set text_expr -%}
      nullif({{ json_expr }} #>> {{ bicam_json_path_literal(path) }}, '')
    {%- endset -%}
    {%- if data_type == "text" -%}
      {{ text_expr }}
    {%- elif data_type == "integer" -%}
      ({{ text_expr }})::integer
    {%- elif data_type == "bigint" -%}
      ({{ text_expr }})::bigint
    {%- elif data_type == "date" -%}
      ({{ text_expr }})::date
    {%- elif data_type == "timestamptz" -%}
      ({{ text_expr }})::timestamptz
    {%- elif data_type == "boolean" -%}
      ({{ text_expr }})::boolean
    {%- else -%}
      {{ exceptions.raise_compiler_error("Unsupported extract_fields type: " ~ data_type) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}

{% macro extract_fields(json_expr, fields) -%}
  {%- for field in fields -%}
    {%- set path = field[0] -%}
    {%- set data_type = field[1] -%}
    {%- set alias = field[2] if field | length > 2 else path.split(".")[-1] -%}
    {{ bicam_typed_json_expr(json_expr, path, data_type) }} as {{ alias }}{% if not loop.last %},{% endif %}
  {%- endfor -%}
{%- endmacro %}
