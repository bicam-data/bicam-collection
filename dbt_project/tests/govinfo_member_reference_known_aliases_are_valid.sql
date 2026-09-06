/*
   Every deterministic GovInfo name alias must resolve to a real Congress.gov
   member.  This protects the small reviewed alias set from silently turning
   into arbitrary string substitutions as source data changes.
*/
select aliases.source_member_reference_key
from {{ ref('int_govinfo_member_reference_candidates') }} as aliases
left join {{ ref('stg_congress__member_detail') }} as members
  on members.bioguide_id = aliases.candidate_bioguide_id
where aliases.candidate_match_basis = 'known_name_alias'
  and (
      aliases.candidate_bioguide_id is null
      or aliases.candidate_bioguide_id !~ '^[A-Z][0-9]{6}$'
      or members.bioguide_id is null
  )
