select *
from {{ ref('bridge_govinfo__bill_versions') }}
where govinfo_source_record_id <> 'govinfo:' || package_id
   or govinfo_raw_id is null
   or govinfo_raw_response_version_id is null
   or (
       link_decision = 'accepted'
       and (
           congress_source_record_id is null
           or congress_raw_id is null
           or congress_raw_response_version_id is null
       )
   )
   or (
       link_decision = 'unmatched'
       and congress_source_record_id is not null
   )
