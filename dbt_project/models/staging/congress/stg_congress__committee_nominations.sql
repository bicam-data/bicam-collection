{{ config(tags=["committees", "staging"]) }}

with keyed as (
    select
        observations.*,
        row_number() over (
            partition by observations.committee_id, observations.nomination_id
            order by observations.source_fetched_at desc nulls last,
                     observations.source_raw_id desc,
                     observations.source_ordinal desc
        ) as key_rank,
        observations.committee_id is not null
            and observations.nomination_id is not null as key_complete
    from {{ ref('int_congress_committee_nomination_observations') }} as observations
)

select committee_id, nomination_id, nomination_number, nomination_part, congress
from keyed
where key_rank = 1 or not key_complete
