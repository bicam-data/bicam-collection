{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- Composite source objects are linkable through their named components, not
-- by copying one person's ID onto the parent object.
with composite_parents as (
    select parent_source_member_reference_key, count(*) as component_count
    from {{ ref('stg_govinfo__member_references') }}
    where reference_kind = 'member_component'
    group by parent_source_member_reference_key
), source_composites as (
    select source_member_reference_key
    from {{ ref('bridge_govinfo__member_references') }}
    where multi_legislator_flag
)
select source_composites.source_member_reference_key
from source_composites
left join composite_parents
  on composite_parents.parent_source_member_reference_key
      = source_composites.source_member_reference_key
where coalesce(composite_parents.component_count, 0) = 0
