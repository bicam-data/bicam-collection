{{ config(tags=["bills", "staging"]) }}

with observations as (
    select *
    from {{ ref('int_congress_bill_committee_observations') }}
), keyed as (
    select
        observations.*,
        row_number() over (
            partition by bill_id, committee_code
            order by source_fetched_at desc nulls last,
                     source_raw_id desc,
                     committee_name asc nulls last
        ) as key_rank,
        bill_id is not null and committee_code is not null as key_complete
    from observations
)
select bill_id, committee_code, committee_name
from keyed
where key_rank = 1 or not key_complete
