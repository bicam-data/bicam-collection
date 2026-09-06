{{ config(tags=["nominations", "staging"]) }}

with unnested as (
    select
        observations.nomination_id,
        observations.nomination_part_id,
        observations.action_id,
        observations.source_fetched_at,
        observations.source_raw_id,
        observations.source_ordinal,
        committee.value as committee_json,
        committee.array_ordinal as committee_ordinal
    from {{ ref('int_congress_nomination_action_observations') }} as observations
    cross join lateral jsonb_array_elements(
        coalesce(observations.action_json -> 'committees', '[]'::jsonb)
    ) with ordinality as committee(value, array_ordinal)
), keyed as (
    select
        unnested.*,
        row_number() over (
            partition by action_id, {{ json_text('committee_json', 'systemCode') }}
            order by source_fetched_at desc nulls last,
                     source_raw_id desc,
                     source_ordinal desc,
                     committee_ordinal desc
        ) as key_rank,
        action_id is not null
            and {{ json_text('committee_json', 'systemCode') }} is not null as key_complete
    from unnested
)

select nomination_id, nomination_part_id, action_id,
       {{ json_text('committee_json', 'systemCode') }} as committee_id,
       {{ json_text('committee_json', 'name') }} as committee_name
from keyed
where key_rank = 1 or not key_complete
