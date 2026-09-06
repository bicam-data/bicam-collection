{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- The bridge must never promote a malformed source value as an identity. The
-- untouched value remains available in bioguide_id_raw for audit/review.
select
    member_reference_bridge_id,
    source_member_reference_key,
    source_bioguide_id
from {{ ref('bridge_govinfo__member_references') }}
where source_bioguide_id is not null
  and source_bioguide_id !~ '^[A-Z][0-9]{6}$'
