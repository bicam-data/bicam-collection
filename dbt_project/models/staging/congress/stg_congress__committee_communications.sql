{{ config(tags=["committees", "staging"]) }}

with keyed as (
    select
        observations.*,
        row_number() over (
            partition by observations.committee_id, observations.communication_id
            order by observations.source_fetched_at desc nulls last,
                     observations.source_raw_id desc,
                     observations.source_ordinal desc
        ) as key_rank,
        observations.committee_id is not null
            and observations.communication_id is not null as key_complete
    from {{ ref('int_congress_committee_communication_observations') }} as observations
)

select committee_id, communication_id, chamber, communication_type_code,
       communication_type_name, congress, communication_number, referral_date,
       updated_at, communication_url
from keyed
where key_rank = 1 or not key_complete
