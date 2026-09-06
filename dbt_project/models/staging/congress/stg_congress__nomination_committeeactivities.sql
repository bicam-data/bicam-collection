{{ config(tags=["nominations", "staging"]) }}

with keyed as (
    select
        observations.*,
        row_number() over (
            partition by observations.nomination_part_id,
                         observations.committee_id,
                         observations.activity_name,
                         observations.activity_date
            order by observations.source_fetched_at desc nulls last,
                     observations.source_raw_id desc,
                     observations.source_ordinal desc,
                     observations.activity_ordinal desc,
                     observations.committee_name desc nulls last
        ) as key_rank,
        observations.nomination_part_id is not null
            and observations.committee_id is not null
            and observations.activity_name is not null
            and observations.activity_date is not null as key_complete
    from {{ ref('int_congress_nomination_committee_activity_observations') }} as observations
)

select nomination_id, nomination_part_id, committee_id, committee_name,
       committee_type, activity_name, activity_date
from keyed
where key_rank = 1 or not key_complete
