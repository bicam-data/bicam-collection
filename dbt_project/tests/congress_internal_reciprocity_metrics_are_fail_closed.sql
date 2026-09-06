/*
   Reciprocity is a release-quality audit, not an informational count.  A
   family with no observed edges, unresolved classifications, endpoint gaps,
   asymmetry, or contradictions must never be reported as passing.  Keep this
   assertion independent of the model's CASE expression so a future edit
   cannot make a vacuous or incomplete family look healthy.
*/
select
    relationship_type,
    relationship_count,
    unavailable_count,
    asymmetric_count,
    contradiction_count,
    unclassified_count,
    quality_status
from {{ ref('fct_congress_internal_reciprocity_metrics') }}
where quality_status = 'pass'
  and (
      relationship_count = 0
      or unavailable_count > 0
      or asymmetric_count > 0
      or contradiction_count > 0
      or unclassified_count > 0
  )
