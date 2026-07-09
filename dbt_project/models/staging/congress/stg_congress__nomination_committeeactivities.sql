{{ config(tags=["nominations", "staging"]) }}

with committee_responses as (
    {{ resolve_child('nomination_detail', 'nomination_committees', 'nominations') }}
),
unnested_committees as (
    select
        substring(parent_endpoint_url from '/nomination/([0-9]+)/')::integer as congress,
        substring(parent_endpoint_url from '/nomination/[0-9]+/([0-9]+)') as nomination_number,
        coalesce(
            substring(parent_endpoint_url from '/nomination/[0-9]+/[0-9]+/([^/?]+)'),
            '00'
        ) as nomination_part,
        committee.value as committee_json
    from committee_responses
    cross join lateral {{ unnest_array('response', 'committees', 'committee') }}
),
unnested_activities as (
    select
        'pn' || nomination_number || '-' || congress::text as nomination_id,
        'pn' || nomination_number || '-' || nomination_part || '-' || congress::text as nomination_part_id,
        committee_json,
        activity.value as activity_json
    from unnested_committees
    cross join lateral jsonb_array_elements(coalesce(committee_json -> 'activities', '[]'::jsonb)) as activity(value)
)

select
    nomination_id,
    nomination_part_id,
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    {{ json_text('committee_json', 'name') }} as committee_name,
    {{ json_text('committee_json', 'type') }} as committee_type,
    case
        when jsonb_typeof(activity_json) = 'string' then nullif(trim(both '"' from activity_json::text), '')
        else nullif(activity_json ->> 'name', '')
    end as activity_name,
    case
        when jsonb_typeof(activity_json) = 'object' then nullif(activity_json ->> 'date', '')::date
        else null
    end as activity_date
from unnested_activities
