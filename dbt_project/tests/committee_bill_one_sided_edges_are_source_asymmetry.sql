{{ config(
    pre_hook=[
        "set local work_mem='512MB'",
        "set local max_parallel_workers_per_gather=4"
    ]
) }}

/*
   The committee-side bills endpoint and bill-side committees endpoint expose
   different source views (committee-authored items versus bill referrals).
   Preserve either direction in the canonical union, but do not call a
   one-sided edge a contradiction when both endpoint records exist.
*/
select relationship_id, relationship_type, left_id, right_id
from {{ ref('fct_congress_internal_reciprocity') }}
where relationship_type = 'committee_bill'
  and (left_present <> right_present)
  and reciprocity_status not in (
      'left_only_asymmetric_source',
      'right_only_asymmetric_source',
      'left_only_counterpart_unavailable',
      'right_only_counterpart_unavailable'
  )
