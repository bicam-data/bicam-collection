select
    source_hearing_id,
    format_package_id,
    public_hearing_id,
    package_candidate_count
from {{ ref('int_congress_hearing_public_id_map') }}
where format_package_id is not null
  and (
      package_candidate_count <> 1
      or public_hearing_id is distinct from {{ bicam_govinfo_hearing_id('format_package_id') }}
  )
