select source_id, target_id
from {{ ref('int_hearing_committee_meeting_links') }}
where relationship_type = 'govinfo_hearing_to_congress_hearing'
  and match_rule = 'govinfo.package_id=congress.congress+jacket_number_fallback'
  and (
       (source_id ~* '^CHRG-[0-9]+hhrg' and target_id !~* '^[hj]hrg')
       or (source_id ~* '^CHRG-[0-9]+shrg' and target_id !~* '^[sj]hrg')
       or (source_id ~* '^CHRG-[0-9]+jhrg' and target_id !~* '^jhrg')
  )
