{% macro govinfo_committeereport_report_set_id(package_id_expr) -%}
case
  when lower({{ package_id_expr }}) ~ '^(gpo-)?crpt-[0-9]+[a-z]+[0-9]+$'
    then regexp_replace(lower({{ package_id_expr }}), '^(gpo-)?crpt-([0-9]+)([a-z]+)([0-9]+)$', '\3\4-\2')
  else null
end
{%- endmacro %}


{#
  A SERIALSET package has two numbers: serialSetNumber is a catalog/volume
  number, while documentNumber is the report number printed in the citation.
  The latter is the report identity component (for example, HRPT 10 in the
  16th Congress), so never concatenate the serial-set number into it.  Keep
  this macro's fourth argument for callers from older projects, but ignore it
  deliberately; the raw serial-set value remains available in staging.
#}
{% macro govinfo_committeereport_report_set_id_from_fields(report_type_expr, document_number_expr, congress_expr, serial_set_number_expr='null') -%}
case
  when {{ report_type_expr }} is not null
   and {{ document_number_expr }} is not null
   and {{ congress_expr }} is not null
    then lower({{ report_type_expr }})
      || {{ document_number_expr }}
      || '-'
      || ({{ congress_expr }})::text
  else null
end
{%- endmacro %}

{% macro govinfo_committeereport_report_id(report_set_id_expr, report_part_expr, is_errata_expr='false') -%}
case
  /* report_set_id is retained as <type><number>-<congress>, while the
     published report ID is <type><number>-<part>-<congress>. */
  when {{ report_set_id_expr }} ~ '^[a-z]+[0-9]+-[0-9]+$' then
    split_part({{ report_set_id_expr }}, '-', 1)
    || '-'
    || ({{ report_part_expr }})::text
    || case when {{ is_errata_expr }} then 'e' else '' end
    || '-'
    || split_part({{ report_set_id_expr }}, '-', 2)
  else
    {{ report_set_id_expr }}
    || '-'
    || ({{ report_part_expr }})::text
    || case when {{ is_errata_expr }} then 'e' else '' end
end
{%- endmacro %}

{% macro govinfo_roman_to_int(expr) -%}
case upper(coalesce({{ expr }}, ''))
  when 'I' then 1
  when 'II' then 2
  when 'III' then 3
  when 'IV' then 4
  when 'V' then 5
  when 'VI' then 6
  when 'VII' then 7
  when 'VIII' then 8
  when 'IX' then 9
  when 'X' then 10
  when 'XI' then 11
  when 'XII' then 12
  when 'XIII' then 13
  when 'XIV' then 14
  when 'XV' then 15
  when 'XVI' then 16
  when 'XVII' then 17
  when 'XVIII' then 18
  when 'XIX' then 19
  when 'XX' then 20
  when 'XXI' then 21
  when 'XXII' then 22
  when 'XXIII' then 23
  when 'XXIV' then 24
  when 'XXV' then 25
  when 'XXVI' then 26
  when 'XXVII' then 27
  when 'XXVIII' then 28
  when 'XXIX' then 29
  when 'XXX' then 30
  when 'XXXI' then 31
  when 'XXXII' then 32
  when 'XXXIII' then 33
  when 'XXXIV' then 34
  when 'XXXV' then 35
  when 'XXXVI' then 36
  when 'XXXVII' then 37
  when 'XXXVIII' then 38
  when 'XXXIX' then 39
  when 'XL' then 40
  when 'XLI' then 41
  when 'XLII' then 42
  when 'XLIII' then 43
  when 'XLIV' then 44
  when 'XLV' then 45
  when 'XLVI' then 46
  when 'XLVII' then 47
  when 'XLVIII' then 48
  when 'XLIX' then 49
  when 'L' then 50
  else null
end
{%- endmacro %}

{% macro govinfo_committeereport_heading_part(heading_expr, granule_class_expr='null') -%}
coalesce(
  case
    when substring(coalesce({{ heading_expr }}, '') from '(?i)\b(?:part|book|volume)\s+([0-9]+)\b') is not null
      then substring(coalesce({{ heading_expr }}, '') from '(?i)\b(?:part|book|volume)\s+([0-9]+)\b')::integer
    else null
  end,
  {{ govinfo_roman_to_int("substring(coalesce(" ~ heading_expr ~ ", '') from '(?i)\\b(?:part|book|volume)\\s+([ivxlcdm]+)\\b')") }},
  case
    when lower(coalesce({{ granule_class_expr }}, '')) = 'firstpart' then 1
    when lower(coalesce({{ granule_class_expr }}, '')) = 'secondpart' then 2
    when lower(coalesce({{ granule_class_expr }}, '')) = 'thirdpart' then 3
    when lower(coalesce({{ granule_class_expr }}, '')) = 'fourthpart' then 4
    when lower(coalesce({{ granule_class_expr }}, '')) = 'fifthpart' then 5
    when lower(coalesce({{ granule_class_expr }}, '')) = 'sixthpart' then 6
    when lower(coalesce({{ granule_class_expr }}, '')) = 'seventhpart' then 7
    when lower(coalesce({{ granule_class_expr }}, '')) = 'eighthpart' then 8
    when lower(coalesce({{ granule_class_expr }}, '')) = 'ninthpart' then 9
    when lower(coalesce({{ granule_class_expr }}, '')) = 'tenthpart' then 10
    else null
  end
)
{%- endmacro %}

{% macro govinfo_committeereport_granule_id_part(granule_id_expr, congress_expr='null') -%}
case
  when substring(coalesce({{ granule_id_expr }}, '') from '(?i)-pt([0-9]+)(?:_[0-9]+)?$') is not null then
    case
      when {{ congress_expr }} is not null
       and substring(coalesce({{ granule_id_expr }}, '') from '(?i)-pt([0-9]+)(?:_[0-9]+)?$')::integer = {{ congress_expr }}
        then 1
      when {{ congress_expr }} is not null
       and substring(coalesce({{ granule_id_expr }}, '') from '(?i)-pt[0-9]+_([0-9]+)$') is not null
       and substring(coalesce({{ granule_id_expr }}, '') from '(?i)-pt[0-9]+_([0-9]+)$')::integer = {{ congress_expr }}
        then 1
      else substring(coalesce({{ granule_id_expr }}, '') from '(?i)-pt([0-9]+)(?:_[0-9]+)?$')::integer
    end
  else null
end
{%- endmacro %}

{% macro govinfo_committeereport_report_part(part_number_expr, heading_expr, granule_id_expr='null', congress_expr='null', granule_class_expr='null') -%}
coalesce(
  {{ part_number_expr }},
  {{ govinfo_committeereport_heading_part(heading_expr, granule_class_expr) }},
  {{ govinfo_committeereport_granule_id_part(granule_id_expr, congress_expr) }},
  1
)
{%- endmacro %}

{% macro govinfo_committeereport_is_errata(title_expr, heading_expr='null', granule_id_expr='null') -%}
(
  coalesce({{ heading_expr }}, '') ~* 'errata'
  or coalesce({{ title_expr }}, '') ~* 'errata'
  or lower(coalesce({{ granule_id_expr }}, '')) like '%err%'
)
{%- endmacro %}
