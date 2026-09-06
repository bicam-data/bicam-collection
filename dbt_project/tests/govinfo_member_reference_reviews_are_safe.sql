/* Non-federal dispositions are only valid for hearing references with no
   source Bioguide and no Congress.gov candidate. They must never mask a
   potentially linkable member row. */
select reviews.source_member_reference_key
from {{ ref('govinfo_member_reference_reviews') }} as reviews
join {{ ref('bridge_govinfo__member_references') }} as refs
  on refs.source_member_reference_key = reviews.source_member_reference_key
where refs.category <> 'govinfo-hearings'
   or refs.identity_context <> 'hearing_committee_member'
   or refs.source_bioguide_id is not null
   or refs.candidate_count <> 0
   or refs.review_disposition is distinct from reviews.classification
