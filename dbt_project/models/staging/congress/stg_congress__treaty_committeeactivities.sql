{{ config(tags=["treaties", "staging"]) }}

with unnested_activities as (
    select
        observations.treaty_id,
        observations.treaty_part_id,
        observations.source_fetched_at,
        observations.source_raw_id,
        observations.source_ordinal,
        observations.committee_json,
        activity.value as activity_json,
        activity.array_ordinal as activity_ordinal
    from {{ ref('int_congress_treaty_committee_observations') }} as observations
    cross join lateral jsonb_array_elements(
        coalesce(observations.committee_json -> 'activities', '[]'::jsonb)
    ) with ordinality as activity(value, array_ordinal)
), extracted as (
    select
        treaty_id,
        treaty_part_id,
        source_fetched_at,
        source_raw_id,
        source_ordinal,
        activity_ordinal,
        {{ json_text('committee_json', 'systemCode') }} as committee_id,
        {{ json_text('committee_json', 'name') }} as committee_name,
        {{ json_text('committee_json', 'type') }} as committee_type,
        lower({{ json_text('committee_json', 'chamber') }}) as chamber,
        case
            when jsonb_typeof(activity_json) = 'string'
                then nullif(trim(both '"' from activity_json::text), '')
            else nullif(activity_json ->> 'name', '')
        end as activity_name,
        case
            when jsonb_typeof(activity_json) = 'object'
                then nullif(activity_json ->> 'date', '')::date
            else null
        end as activity_date
    from unnested_activities
), keyed as (
    select
        extracted.*,
        row_number() over (
            partition by treaty_part_id, committee_id, activity_name, activity_date
            order by source_fetched_at desc nulls last,
                     source_raw_id desc,
                     source_ordinal desc,
                     activity_ordinal desc,
                     committee_name desc nulls last
        ) as key_rank,
        treaty_part_id is not null
            and committee_id is not null
            and activity_name is not null
            and activity_date is not null as key_complete
    from extracted
)

select treaty_id, treaty_part_id, committee_id, committee_name, committee_type,
       chamber, activity_name, activity_date
from keyed
where key_rank = 1 or not key_complete
