/* Every component override must target an observed candidate from the same
   component. A reviewed override may disambiguate a candidate that was
   initially ambiguous, but it must not introduce an ID absent from the
   candidate evidence lineage. */
select overrides.component_key, overrides.candidate_bioguide_id
from {{ ref('govinfo_member_component_identity_overrides') }} as overrides
left join {{ ref('int_govinfo_multi_legislator_candidates') }} as candidates
 on candidates.component_key = overrides.component_key
 and candidates.candidate_bioguide_id = upper(overrides.candidate_bioguide_id::text)
 and candidates.resolution_status in (
       'unique_name_congress_candidate',
       'ambiguous_candidate',
       'source_bioguide_evidence'
     )
where candidates.component_key is null
