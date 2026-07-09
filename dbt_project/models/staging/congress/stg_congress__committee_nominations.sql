{{ config(tags=["committees", "staging"]) }}

with nomination_responses as (
    {{ resolve_child('committee_detail', 'committee_nominations', 'committees') }}
),
unnested as (
    select
        parent_raw_id,
        nomination.value as nomination_json
    from nomination_responses
    cross join lateral {{ unnest_array('response', 'nominations', 'nomination') }}
),
committee_lookup as (
    select
        id as raw_id,
        response -> 'committee' as committee_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'committees'
      and node_type = 'committee_detail'
)

select
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    'pn'
    || {{ json_text('nomination_json', 'number') }}
    || '-'
    || coalesce({{ json_text('nomination_json', 'partNumber') }}, '00')
    || '-'
    || {{ json_int('nomination_json', 'congress') }}::text as nomination_id,
    {{ json_int('nomination_json', 'number') }} as nomination_number,
    coalesce({{ json_text('nomination_json', 'partNumber') }}, '00') as nomination_part,
    {{ json_int('nomination_json', 'congress') }} as congress
from unnested
inner join committee_lookup
    on unnested.parent_raw_id = committee_lookup.raw_id
