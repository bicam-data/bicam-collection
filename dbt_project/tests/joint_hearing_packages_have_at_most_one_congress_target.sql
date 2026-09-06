select source_id, count(distinct target_id) as congress_target_count
from {{ ref('int_hearing_committee_meeting_links') }}
where source_id ~* '^CHRG-[0-9]+jhrg'
  and relationship_type = 'govinfo_hearing_to_congress_hearing'
group by source_id
having count(distinct target_id) > 1
