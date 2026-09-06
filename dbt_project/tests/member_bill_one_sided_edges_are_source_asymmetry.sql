{{ config(
    pre_hook=[
        "set local work_mem='512MB'",
        "set local max_parallel_workers_per_gather=4"
    ]
) }}

/*
   Member legislation lists and bill sponsor/cosponsor lists are both source
   evidence. A one-sided edge remains in the canonical union, but it is not a
   contradiction: historical endpoint coverage is not bijective.
*/
select relationship_id, relationship_type, left_id, right_id
from {{ ref('fct_congress_internal_reciprocity') }}
where relationship_type in ('member_bill_sponsor', 'member_bill_cosponsor')
  and (left_present <> right_present)
  and reciprocity_status not in (
      'left_only_asymmetric_source',
      'right_only_asymmetric_source',
      'left_only_counterpart_unavailable',
      'right_only_counterpart_unavailable'
  )
