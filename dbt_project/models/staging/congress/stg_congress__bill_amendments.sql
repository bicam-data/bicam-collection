{{ config(materialized="table", tags=["bills", "staging"]) }}

with observations as (
    select *
    from {{ ref('int_congress_bill_amendment_observations') }}
), keyed as (
    select
        observations.*,
        row_number() over (
            partition by bill_id, amendment_id
            order by source_fetched_at desc nulls last,
                     source_raw_id desc,
                     amendment_type asc nulls last,
                     amendment_number asc nulls last,
                     congress asc nulls last
        ) as key_rank,
        bill_id is not null and amendment_id is not null as key_complete
    from observations
)
select bill_id, amendment_id, amendment_type, amendment_number, congress
from keyed
where key_rank = 1 or not key_complete
