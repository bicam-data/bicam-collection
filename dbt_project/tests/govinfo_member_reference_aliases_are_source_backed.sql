{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- Every source-reference alias must be backed by the same display name and a
-- valid Bioguide in another GovInfo reference. Aliases never invent IDs.
select
    candidates.source_member_reference_key,
    candidates.parsed_name,
    candidates.candidate_bioguide_id
from {{ ref('int_govinfo_member_reference_candidates') }} as candidates
where candidates.candidate_match_basis = 'source_reference_alias'
  and (
      candidates.candidate_bioguide_id !~ '^[A-Z][0-9]{6}$'
      or not exists (
          select 1
          from {{ ref('int_govinfo_member_references') }} as source_refs
          where lower(trim(source_refs.parsed_name))
              = lower(trim(candidates.parsed_name))
            and source_refs.bioguide_id_raw = candidates.candidate_bioguide_id
      )
  )
