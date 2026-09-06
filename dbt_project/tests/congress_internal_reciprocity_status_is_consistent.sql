{{ config(
    pre_hook=[
        "set local work_mem='512MB'",
        "set local max_parallel_workers_per_gather=4"
    ]
) }}

/*
   Every status must agree with the evidence flags. One-sided relationships
   with both endpoint records available are source asymmetry; one-sided rows
   with an unavailable endpoint are counterpart-unavailable. A contradiction
   is reserved for a future attribute/value conflict and, if present, must
   still have both endpoint records and one-sided source evidence.
*/
select relationship_id, reciprocity_status
from {{ ref('fct_congress_internal_reciprocity') }}
where (reciprocity_status = 'reciprocal'
       and not (left_present and right_present))
   or (reciprocity_status = 'left_only_asymmetric_source'
       and not (
           left_present and not right_present
           and left_endpoint_available and right_endpoint_available
       ))
   or (reciprocity_status = 'right_only_asymmetric_source'
       and not (
           right_present and not left_present
           and left_endpoint_available and right_endpoint_available
       ))
   or (reciprocity_status = 'left_only_counterpart_unavailable'
       and not (
           left_present and not right_present
           and not (left_endpoint_available and right_endpoint_available)
       ))
   or (reciprocity_status = 'right_only_counterpart_unavailable'
       and not (
           right_present and not left_present
           and not (left_endpoint_available and right_endpoint_available)
       ))
   or (reciprocity_status = 'left_only_contradiction'
       and not (
           left_present and not right_present
           and left_endpoint_available and right_endpoint_available
       ))
   or (reciprocity_status = 'right_only_contradiction'
       and not (
           right_present and not left_present
           and left_endpoint_available and right_endpoint_available
       ))
