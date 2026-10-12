{# Publication backstop for cached successes, requiring no new runtime table.
   Missing headings remain unverified, not automatically contradictory. #}
{% macro bill_text_has_congress_conflict(text_expr, bill_id_expr) -%}
coalesce(
    (regexp_match(left({{ text_expr }}, 1600),
       '(?in)^[ \t]*([0-9]+)(?:st|nd|rd|th|d)?[ \t]+CONGRESS[ \t]*$'))[1]::integer
    <> split_part({{ bill_id_expr }}, '-', 2)::integer,
    false
)
{%- endmacro %}
