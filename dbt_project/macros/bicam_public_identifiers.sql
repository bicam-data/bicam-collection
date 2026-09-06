{% macro bicam_public_committee_meeting_id(expression) -%}
    regexp_replace(({{ expression }})::text, '-[0-9]+$', '')
{%- endmacro %}

{% macro bicam_public_nomination_id(expression) -%}
    upper(({{ expression }})::text)
{%- endmacro %}
