{{ config(tags=["nominations", "staging"]) }}

with keyed as (
    select
        observations.*,
        row_number() over (
            partition by observations.action_id
            order by observations.source_fetched_at desc nulls last,
                     observations.source_raw_id desc,
                     observations.source_ordinal desc
        ) as key_rank,
        observations.action_id is not null
            and observations.nomination_part_id is not null as key_complete
    from {{ ref('int_congress_nomination_action_observations') }} as observations
)

select nomination_id, nomination_part_id, action_id, action_code, action_type,
       action_date, text
from keyed
where key_rank = 1 or not key_complete
