{% macro collection_partition_value(endpoint_expression, category_expression) -%}
case
    when {{ category_expression }} in (
        'amendments',
        'bills',
        'committee-meetings',
        'committee-prints',
        'committee-reports',
        'committees',
        'hearings',
        'house-communications',
        'house-votes',
        'laws',
        'nominations',
        'senate-communications',
        'treaties'
    ) then coalesce(
        (
            regexp_match(
                {{ endpoint_expression }},
                '/(?:amendment|bill|committee-meeting|committee-print|committee-report|committee|hearing|house-communication|house-vote|law|nomination|senate-communication|treaty)/([0-9]{1,3})(?:/|\\?|$)'
            )
        )[1],
        'unpartitioned'
    )
    else 'unpartitioned'
end
{%- endmacro %}
