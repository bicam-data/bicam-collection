/* A source Bioguide may identify only one component of a composite object. */
select
    components.component_key,
    components.resolved_bioguide_id,
    refs.source_bioguide_id
from {{ ref('bridge_govinfo__member_reference_components') }} as components
join {{ ref('bridge_govinfo__member_references') }} as refs using (source_member_reference_key)
where components.resolution_status = 'source_bioguide_evidence'
  and components.resolved_bioguide_id <> refs.source_bioguide_id
