/* Candidate evidence may only cross-reference the chamber stated by the
   GovInfo source object. A null candidate chamber is also a violation when a
   source chamber is present, because it would make the evidence unverifiable.
*/
with candidate_rows as (
    select
        source_member_key,
        chamber as source_chamber,
        candidate_bioguide_id,
        candidate_chamber
    from {{ ref('fct_govinfo_member_identity_candidates') }}
    where candidate_bioguide_id is not null
), component_rows as (
    select
        component_key as source_member_key,
        chamber as source_chamber,
        candidate_bioguide_id,
        candidate_chamber
    from {{ ref('int_govinfo_multi_legislator_candidates') }}
    where candidate_bioguide_id is not null
), violations as (
    select * from candidate_rows
    union all
    select * from component_rows
)
select source_member_key, source_chamber, candidate_bioguide_id, candidate_chamber
from violations
where upper(trim(source_chamber)) in ('H', 'S')
  and (
      candidate_chamber is null
      or (upper(trim(source_chamber)) = 'H'
          and lower(trim(candidate_chamber)) <> 'house of representatives')
      or (upper(trim(source_chamber)) = 'S'
          and lower(trim(candidate_chamber)) <> 'senate')
  )
