{% macro amendment_type_from_url(url_expr, congress_expr, amendment_number_expr) -%}
case
    when {{ url_expr }} is null
      or {{ congress_expr }} is null
      or {{ amendment_number_expr }} is null
      or {{ url_expr }} not like '%/amendment/%'
    then null
    else split_part(
        split_part({{ url_expr }}, '/' || {{ congress_expr }}::text || '/', 2),
        '/' || {{ amendment_number_expr }},
        1
    )
end
{%- endmacro %}
