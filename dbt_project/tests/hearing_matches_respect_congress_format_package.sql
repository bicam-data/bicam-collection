select
    matches.govinfo_source_record_id,
    matches.congress_source_record_id,
    congress_ids.format_package_id,
    congress_ids.public_hearing_id as congress_public_hearing_id,
    govinfo_ids.hearing_id as govinfo_public_hearing_id
from {{ ref('fct_hearing_matches') }} as matches
join {{ ref('int_congress_hearing_public_id_map') }} as congress_ids
  on congress_ids.source_hearing_id = matches.congress_source_record_id
join {{ ref('int_govinfo_hearing_identity_map') }} as govinfo_ids
  on govinfo_ids.package_id = matches.govinfo_source_record_id
where matches.decision = 'accepted'
  and congress_ids.format_package_id is not null
  and congress_ids.public_hearing_id is distinct from govinfo_ids.hearing_id
