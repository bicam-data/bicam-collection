{% macro bill_canonical_id(bill_type_expr, bill_number_expr, congress_expr) -%}
case
  when {{ bill_type_expr }} is not null
   and {{ bill_number_expr }} is not null
   and {{ congress_expr }} is not null
    then lower({{ bill_type_expr }})
      || ({{ bill_number_expr }})::text
      || '-'
      || ({{ congress_expr }})::text
  else null
end
{%- endmacro %}

{% macro normalize_chamber(expr) -%}
case
  when lower(trim(coalesce({{ expr }}, ''))) in ('house', 'h') then 'house'
  when lower(trim(coalesce({{ expr }}, ''))) in ('senate', 's') then 'senate'
  when nullif(trim(coalesce({{ expr }}, '')), '') is null then null
  else lower(trim({{ expr }}))
end
{%- endmacro %}
