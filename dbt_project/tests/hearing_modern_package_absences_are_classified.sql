select govinfo_source_record_id, decision_rule, confirmed_source_gap
from {{ ref('fct_hearing_matches') }}
where decision = 'unmatched'
  and govinfo_congress >= 104
  and decision_rule not in (
      'committee_meeting_evidence_only',
      'confirmed_source_gap_404',
      'confirmed_source_gap_no_congress_package_evidence'
  )
