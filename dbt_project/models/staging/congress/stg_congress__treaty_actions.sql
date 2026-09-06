{{ config(tags=["treaties", "staging"]) }}

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
            and observations.treaty_part_id is not null as key_complete
    from {{ ref('int_congress_treaty_action_observations') }} as observations
)

select treaty_id, treaty_part_id, action_id, action_code, action_date, text,
       action_type, committee_id, committee_name
from keyed
where key_rank = 1 or not key_complete
