{{ config(tags=["bills", "staging"]) }}

with keyed as (
    select
        observations.*,
        row_number() over (
            partition by bill_id, relatedbill_id
            order by source_fetched_at desc nulls last,
                     source_raw_id desc,
                     relationship_ordinal desc nulls last,
                     relationship_identified_by asc nulls last,
                     relationship_type asc nulls last
        ) as key_rank,
        bill_id is not null and relatedbill_id is not null as key_complete
    from {{ ref('int_congress_bill_related_bill_observations') }} as observations
)

select bill_id, relatedbill_id, relatedbill_type, relatedbill_number, congress,
       relationship_type, relationship_identified_by
from keyed
where key_rank = 1 or not key_complete
