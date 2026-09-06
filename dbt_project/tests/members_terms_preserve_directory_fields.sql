/* Directory granules carry term-level title, biography, population, and
   online fields that Congress.gov does not expose on its term object. Those
   fields must survive the source-to-public join whenever a CDIR member has a
   matching canonical term. */
with directory_evidence as (
    select distinct
        bridge.linkable_bioguide_id as bioguide_id,
        directory_members.congress,
        case
            when upper(directory_members.chamber) = 'H' then 'house of representatives'
            when upper(directory_members.chamber) = 'S' then 'senate'
            else lower(directory_members.chamber)
        end as chamber,
        directory_members.granule_title,
        directory_members.biography,
        directory_members.population,
        directory_members.online
    from {{ ref('stg_govinfo__congressional_directory_members') }} as directory_members
    join {{ ref('bridge_govinfo__member_references') }} as bridge
      on bridge.source_member_reference_key = directory_members.source_member_reference_key
    where bridge.linkable_bioguide_id is not null
), missing as (
    select evidence.*
    from directory_evidence as evidence
    join {{ ref('members_terms') }} as terms
      on terms.bioguide_id = evidence.bioguide_id
     and terms.congress = evidence.congress
     and terms.chamber = evidence.chamber
    where (evidence.granule_title is not null and terms.title is null)
       or (evidence.biography is not null and terms.biography is null)
       or (evidence.population is not null and terms.population is null)
       or (evidence.online is not null and terms.other_url is null)
)
select * from missing
