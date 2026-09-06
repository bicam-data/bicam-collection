select refs.source_member_reference_key
from {{ ref('bridge_govinfo__member_references') }} as refs
join {{ ref('fct_govinfo_member_reference_identity_exceptions') }} as queue
  on queue.member_reference_bridge_id = refs.member_reference_bridge_id
where refs.review_disposition = 'reviewed_non_federal_participant'
