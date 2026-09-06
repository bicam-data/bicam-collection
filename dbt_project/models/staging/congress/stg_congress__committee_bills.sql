{{ config(tags=["committees", "staging"]) }}

with observations as (
    select *
    from {{ ref('int_congress_committee_bill_observations') }}
), keyed as (
    select
        observations.*,
        row_number() over (
            partition by committee_id, bill_id
            order by source_fetched_at desc nulls last,
                     source_raw_id desc,
                     updated_at desc nulls last,
                     relationship_type asc nulls last
        ) as key_rank,
        committee_id is not null and bill_id is not null as key_complete
    from observations
)
select
    committee_id,
    bill_id,
    bill_type,
    bill_number,
    congress,
    relationship_type,
    committee_action_date,
    updated_at
from keyed
where key_rank = 1 or not key_complete
