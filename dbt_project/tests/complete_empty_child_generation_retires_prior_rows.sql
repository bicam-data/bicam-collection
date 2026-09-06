/*
   The fixture corpus contains a non-empty full bill-subject response followed
   by an empty full response under reordered transport parameters. The later
   complete generation must be selected, while the earlier source observation
   remains available in immutable-version evidence.
*/
with selected as (
    select *
    from {{ ref('int_congress_child_response_selections') }}
    where category = 'bills'
      and node_type = 'bill_subjects'
      and logical_collection like '%/bill/118/hr/2/subjects%'
), public_observations as (
    select *
    from {{ ref('int_congress_bill_subject_observations') }}
    where bill_id = 'hr2-118'
)
select logical_collection, array_item_count, is_complete_generation
from selected
where not (array_item_count = 0 and is_complete_generation)
union all
select 'public_observation' as logical_collection, count(*)::bigint, false
from public_observations
having count(*) <> 0
