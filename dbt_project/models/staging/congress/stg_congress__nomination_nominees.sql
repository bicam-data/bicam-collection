{{ config(tags=["nominations", "staging"]) }}

with nominee_responses as (
    {{ resolve_child('nomination_detail', 'nomination_nominee_detail', 'nominations') }}
),
unnested as (
    select
        substring(parent_endpoint_url from '/nomination/([0-9]+)/')::integer as congress,
        substring(parent_endpoint_url from '/nomination/[0-9]+/([0-9]+)') as nomination_number,
        coalesce(
            substring(parent_endpoint_url from '/nomination/[0-9]+/[0-9]+/([^/?]+)'),
            '00'
        ) as nomination_part,
        nominee.value as nominee_json
    from nominee_responses
    cross join lateral {{ unnest_array('response', 'nominees', 'nominee') }}
)

select
    'pn' || nomination_number || '-' || congress::text as nomination_id,
    'pn' || nomination_number || '-' || nomination_part || '-' || congress::text as nomination_part_id,
    {{ json_int('nominee_json', 'ordinal') }} as position_id,
    {{ json_int('nominee_json', 'ordinal') }} as nominee_ordinal,
    {{ json_text('nominee_json', 'firstName') }} as first_name,
    {{ json_text('nominee_json', 'middleName') }} as middle_name,
    {{ json_text('nominee_json', 'lastName') }} as last_name,
    {{ json_text('nominee_json', 'prefix') }} as prefix,
    {{ json_text('nominee_json', 'suffix') }} as suffix,
    {{ json_text('nominee_json', 'state') }} as state,
    {{ json_date('nominee_json', 'effectiveDate') }} as effective_date,
    {{ json_text('nominee_json', 'predecessorName') }} as predecessor_name,
    {{ json_text('nominee_json', 'corpsCode') }} as corps_code
from unnested
