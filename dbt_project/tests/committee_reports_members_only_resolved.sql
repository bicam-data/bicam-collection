/* Public member edges may only contain identities resolved by the GovInfo
   identity bridge and linked to the canonical members relation. */
select public_members.report_id, public_members.bioguide_id
from {{ ref('committee_reports_members') }} as public_members
left join {{ ref('bridge_govinfo__member_references') }} as resolved
  on resolved.resolved_bioguide_id = public_members.bioguide_id
where resolved.resolved_bioguide_id is null
