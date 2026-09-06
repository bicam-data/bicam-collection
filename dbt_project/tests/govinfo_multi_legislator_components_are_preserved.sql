/* Every source member object flagged as multi-legislator must produce at
   least two named components in the evidence layer. */
with source_component_counts as (
    select source_member_key
    from {{ ref('int_govinfo_multi_legislator_candidates') }}
    group by source_member_key
    having count(distinct component_ordinal) < 2
), flagged as (
    select source_member_key
    from {{ ref('stg_govinfo__committeereports_members') }}
    where multi_legislator_flag
)
select flagged.source_member_key
from flagged
left join source_component_counts using (source_member_key)
where source_component_counts.source_member_key is not null
