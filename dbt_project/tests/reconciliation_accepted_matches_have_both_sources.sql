select 'bill' as entity, match_id::text as match_id
from {{ ref('fct_bill_matches') }}
where decision = 'accepted'
  and (
       congress_source_record_id is null
       or govinfo_source_record_id is null
       or congress_raw_id is null
       or congress_raw_response_version_id is null
       or congress_endpoint_url is null
       or govinfo_raw_id is null
       or govinfo_raw_response_version_id is null
       or govinfo_endpoint_url is null
  )

union all

select 'committee_report' as entity, match_id::text as match_id
from {{ ref('fct_committee_report_matches') }}
where decision = 'accepted'
  and (
       congress_source_record_id is null
       or govinfo_source_record_id is null
       or congress_raw_id is null
       or congress_raw_response_version_id is null
       or congress_endpoint_url is null
       or govinfo_raw_id is null
       or govinfo_raw_response_version_id is null
       or govinfo_endpoint_url is null
  )

union all

select 'hearing' as entity, match_id::text as match_id
from {{ ref('fct_hearing_matches') }}
where decision = 'accepted'
  and (
       congress_source_record_id is null
       or govinfo_source_record_id is null
       or congress_raw_id is null
       or congress_raw_response_version_id is null
       or congress_endpoint_url is null
       or govinfo_raw_id is null
       or govinfo_raw_response_version_id is null
       or govinfo_endpoint_url is null
  )
