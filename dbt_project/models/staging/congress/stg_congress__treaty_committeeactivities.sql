{{ config(tags=["treaties", "staging"]) }}

with committee_responses as (
    {{ resolve_child('treaty_detail', 'treaty_committees', 'treaties') }}
),
unnested_committees as (
    select
        substring(parent_endpoint_url from '/treaty/([0-9]+)/')::integer as congress,
        substring(parent_endpoint_url from '/treaty/[0-9]+/([0-9]+)') as treaty_number,
        coalesce(
            substring(parent_endpoint_url from '/treaty/[0-9]+/[0-9]+/([^/?]+)'),
            ''
        ) as treaty_suffix,
        committee.value as committee_json
    from committee_responses
    cross join lateral {{ unnest_array('response', 'treatyCommittees', 'committee') }}
),
unnested_activities as (
    select
        'td' || congress::text || '-' || treaty_number as treaty_id,
        'td' || congress::text || '-' || treaty_number || treaty_suffix as treaty_part_id,
        committee_json,
        activity.value as activity_json
    from unnested_committees
    cross join lateral jsonb_array_elements(coalesce(committee_json -> 'activities', '[]'::jsonb)) as activity(value)
)

select
    treaty_id,
    treaty_part_id,
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    {{ json_text('committee_json', 'name') }} as committee_name,
    {{ json_text('committee_json', 'type') }} as committee_type,
    lower({{ json_text('committee_json', 'chamber') }}) as chamber,
    case
        when jsonb_typeof(activity_json) = 'string' then nullif(trim(both '"' from activity_json::text), '')
        else nullif(activity_json ->> 'name', '')
    end as activity_name,
    case
        when jsonb_typeof(activity_json) = 'object' then nullif(activity_json ->> 'date', '')::date
        else null
    end as activity_date
from unnested_activities
