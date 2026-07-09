{% macro build_custom_id(parts, separator='') -%}
concat(
  {%- for part in parts -%}
    coalesce(({{ part }})::text, '')
    {%- if not loop.last -%},
    '{{ separator }}',
    {%- endif -%}
  {%- endfor -%}
)
{%- endmacro %}
