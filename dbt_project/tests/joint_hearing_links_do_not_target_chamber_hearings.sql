select source_id, target_id
from {{ ref('int_hearing_committee_meeting_links') }}
where source_id ~* '^CHRG-[0-9]+jhrg'
  and target_id ~* '^[hs]hrg'
