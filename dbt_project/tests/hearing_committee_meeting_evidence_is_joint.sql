select
    govinfo_source_record_id,
    chamber,
    decision,
    committee_meeting_evidence_count,
    congress_source_record_id,
    congress_raw_id,
    congress_raw_response_version_id,
    congress_endpoint_url
from {{ ref('fct_hearing_matches') }}
where decision_rule = 'committee_meeting_evidence_only'
  and (
       chamber <> 'joint'
       or decision <> 'unmatched'
       or committee_meeting_evidence_count < 1
       or congress_source_record_id is not null
       or congress_raw_id is not null
       or congress_raw_response_version_id is not null
       or congress_endpoint_url is not null
  )
