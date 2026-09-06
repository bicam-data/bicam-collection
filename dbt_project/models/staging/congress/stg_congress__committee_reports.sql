{{ config(tags=["committees", "staging"]) }}

with keyed as (
    select
        observations.*,
        row_number() over (
            partition by committee_id, report_id, report_part_id
            order by source_fetched_at desc nulls last,
                     source_raw_id desc,
                     source_ordinal desc
        ) as key_rank,
        committee_id is not null
            and report_id is not null
            and report_part_id is not null as key_complete
    from {{ ref('int_congress_committee_report_observations') }} as observations
)
select committee_id, report_id, report_part_id
from keyed
where key_rank = 1 or not key_complete
