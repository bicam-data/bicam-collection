{% macro committee_report_set_id(report_type_expr, report_number_expr, congress_expr) -%}
case
  when {{ report_type_expr }} is not null
   and {{ report_number_expr }} is not null
   and {{ congress_expr }} is not null
    then lower({{ report_type_expr }})
      || ({{ report_number_expr }})::text
      || '-'
      || ({{ congress_expr }})::text
  else null
end
{%- endmacro %}

{% macro committee_report_canonical_id(report_type_expr, report_number_expr, congress_expr, report_part_expr='1', is_errata_expr='false') -%}
{{ committee_report_set_id(report_type_expr, report_number_expr, congress_expr) }}
|| '-'
|| coalesce(({{ report_part_expr }})::text, '1')
|| case when coalesce({{ is_errata_expr }}, false) then 'e' else '' end
{%- endmacro %}

{% macro normalize_match_text(expr) -%}
nullif(regexp_replace(lower(coalesce({{ expr }}, '')), '[^a-z0-9]+', '', 'g'), '')
{%- endmacro %}

