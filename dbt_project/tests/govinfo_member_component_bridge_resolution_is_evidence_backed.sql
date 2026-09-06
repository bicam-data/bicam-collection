/* A resolved ID is allowed only for source-ID evidence or a reviewed component
   override that is present in the durable override seed. */
select component_key
from {{ ref('bridge_govinfo__member_components') }}
where resolved_bioguide_id is not null
  and (
      (resolution_status = 'source_bioguide_evidence' and not has_source_bioguide_evidence)
      or (
          resolution_status = 'manual_component_override'
          and manual_override_bioguide_id is null
      )
      or resolution_status not in ('source_bioguide_evidence', 'manual_component_override')
  )
