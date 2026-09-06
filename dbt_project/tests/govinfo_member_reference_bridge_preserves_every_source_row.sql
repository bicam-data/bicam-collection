{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- The link layer is one row per source member object; no source reference may
-- disappear while candidates or overrides are applied.
with source_rows as (
    select source_member_reference_key
    from {{ ref('int_govinfo_member_references') }}
), bridge_rows as (
    select source_member_reference_key
    from {{ ref('bridge_govinfo__member_references') }}
)
select source_rows.source_member_reference_key
from source_rows
left join bridge_rows using (source_member_reference_key)
where bridge_rows.source_member_reference_key is null
union all
select bridge_rows.source_member_reference_key
from bridge_rows
left join source_rows using (source_member_reference_key)
where source_rows.source_member_reference_key is null
