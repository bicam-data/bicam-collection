{% macro hash_to_uuid(expr) -%}
(
  substr(md5({{ expr }}), 1, 8)
  || '-'
  || substr(md5({{ expr }}), 9, 4)
  || '-'
  || substr(md5({{ expr }}), 13, 4)
  || '-'
  || substr(md5({{ expr }}), 17, 4)
  || '-'
  || substr(md5({{ expr }}), 21, 12)
)::uuid
{%- endmacro %}
