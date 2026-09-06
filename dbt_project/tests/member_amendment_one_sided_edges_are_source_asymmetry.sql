{{ config(
    pre_hook=[
        "set local work_mem='512MB'",
        "set local max_parallel_workers_per_gather=4"
    ]
) }}

/* Historical member legislation lists and amendment sponsor/cosponsor lists
   are both source evidence. Preserve one-sided edges in the union and classify
   them as source asymmetry when both endpoint records exist. */
select relationship_id, relationship_type, left_id, right_id
from {{ ref('fct_congress_internal_reciprocity') }}
where relationship_type in (
    'member_amendment_sponsor', 'member_amendment_cosponsor'
  )
  and (left_present <> right_present)
  and reciprocity_status not in (
      'left_only_asymmetric_source',
      'right_only_asymmetric_source',
      'left_only_counterpart_unavailable',
      'right_only_counterpart_unavailable'
  )
