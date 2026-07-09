{% macro json_text(json_expr, path) -%}
  {{ bicam_typed_json_expr(json_expr, path, "text") }}
{%- endmacro %}

{% macro json_int(json_expr, path, default=None) -%}
  {%- if default is none -%}
    {{ bicam_typed_json_expr(json_expr, path, "integer") }}
  {%- else -%}
    coalesce({{ bicam_typed_json_expr(json_expr, path, "integer") }}, {{ default }})
  {%- endif -%}
{%- endmacro %}

{% macro json_bigint(json_expr, path, default=None) -%}
  {%- if default is none -%}
    {{ bicam_typed_json_expr(json_expr, path, "bigint") }}
  {%- else -%}
    coalesce({{ bicam_typed_json_expr(json_expr, path, "bigint") }}, {{ default }})
  {%- endif -%}
{%- endmacro %}

{% macro json_date(json_expr, path) -%}
  {{ bicam_typed_json_expr(json_expr, path, "date") }}
{%- endmacro %}

{% macro json_timestamptz(json_expr, path) -%}
  {{ bicam_typed_json_expr(json_expr, path, "timestamptz") }}
{%- endmacro %}

{% macro json_boolean(json_expr, path, default=None) -%}
  {%- if default is none -%}
    {{ bicam_typed_json_expr(json_expr, path, "boolean") }}
  {%- else -%}
    coalesce({{ bicam_typed_json_expr(json_expr, path, "boolean") }}, {{ default }})
  {%- endif -%}
{%- endmacro %}
