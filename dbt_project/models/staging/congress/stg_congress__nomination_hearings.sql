{{ config(tags=["nominations", "staging"]) }}

with keyed as (
    select
        observations.*,
        row_number() over (
            partition by observations.nomination_part_id, observations.hearing_part_id
            order by observations.source_fetched_at desc nulls last,
                     observations.source_raw_id desc,
                     observations.source_ordinal desc
        ) as key_rank,
        observations.nomination_part_id is not null
            and observations.hearing_part_id is not null as key_complete
    from {{ ref('int_congress_nomination_hearing_observations') }} as observations
)

select nomination_id, nomination_part_id, hearing_id, hearing_part_id, chamber,
       hearing_jacketnumber, congress, hearing_number, hearing_part, hearing_date,
       "errataNumber", citation
from keyed
where key_rank = 1 or not key_complete
