{% macro congress_child_logical_endpoint(endpoint_expression) -%}
/* Remove transport-only query parameters while preserving unknown identity
   parameters. Keep this expression shared by selection and gap reporting so
   a date-window/page variation cannot create duplicate queue work. */
regexp_replace(
    regexp_replace(
        regexp_replace(
            {{ endpoint_expression }},
            '[?&](format|limit|offset|fromDateTime|toDateTime|api_key)=[^&]*',
            '',
            'g'
        ),
        '[?]&',
        '?',
        'g'
    ),
    '[?&]$',
    ''
)
{%- endmacro %}
