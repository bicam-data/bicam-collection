{% macro congress_hearing_route_chamber(chamber_expr) -%}
case
  when lower({{ chamber_expr }}) = 'nochamber' then 'joint'
  else lower({{ chamber_expr }})
end
{%- endmacro %}

{% macro congress_hearing_canonical_chamber(hearing_json_expr) -%}
case
  when lower({{ json_text(hearing_json_expr, 'chamber') }}) = 'nochamber'
    or {{ json_text(hearing_json_expr, 'citation') }} ~* '^J[.]Hrg[.]'
    or exists (
    select 1
    from jsonb_array_elements(
      case
        when jsonb_typeof({{ hearing_json_expr }} -> 'committees') = 'array'
          then {{ hearing_json_expr }} -> 'committees'
        else '[]'::jsonb
      end
    ) as hearing_committee(value)
    where hearing_committee.value ->> 'url' ilike '%/committee/joint/%'
  ) then 'joint'
  else lower({{ json_text(hearing_json_expr, 'chamber') }})
end
{%- endmacro %}
