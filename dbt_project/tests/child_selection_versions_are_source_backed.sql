/* Every reusable child-selection row must point to the exact immutable
   payload version that supplied its JSON and retain that version's raw URL
   identity.  This prevents a current-row replacement from silently severing
   historical child lineage. */
select
    selected.raw_response_version_id,
    selected.raw_id,
    selected.endpoint_url
from {{ ref('int_congress_child_response_selection') }} as selected
left join {{ ref('int_raw_response_versions') }} as versions
  on versions.raw_response_version_id = selected.raw_response_version_id
where versions.raw_response_version_id is null
   or selected.raw_id is distinct from versions.raw_response_id
   or selected.endpoint_url is distinct from versions.endpoint_url
