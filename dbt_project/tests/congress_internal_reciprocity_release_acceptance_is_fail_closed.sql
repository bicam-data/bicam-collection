/*
   The diagnostic quality_status intentionally remains review for source-side
   asymmetry.  Release acceptance is a separate, explicit contract: only
   classifiable families with no contradiction are accepted.  This test keeps
   the policy fail-closed if either the model CASE or a future relationship
   family changes.
*/
select
    relationship_type,
    relationship_count,
    contradiction_count,
    unclassified_count,
    release_acceptance_status
from {{ ref('fct_congress_internal_reciprocity_metrics') }}
where (release_acceptance_status = 'accepted_with_source_boundaries'
       and (
           relationship_count = 0
           or contradiction_count > 0
           or unclassified_count > 0
       ))
   or (release_acceptance_status in (
           'reject_no_evidence',
           'reject_contradiction',
           'reject_unclassified'
       )
       and relationship_count > 0
       and contradiction_count = 0
       and unclassified_count = 0)
