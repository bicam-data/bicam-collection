/* A reviewed participant disposition must refer to a current immutable
   GovInfo member-reference row. */
select reviews.source_member_reference_key
from {{ ref('govinfo_member_reference_reviews') }} as reviews
left join {{ ref('bridge_govinfo__member_references') }} as refs
  on refs.source_member_reference_key = reviews.source_member_reference_key
where refs.source_member_reference_key is null
