{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committees', 'members'],
    indexes=[
      {'columns': ['committee_code', 'bioguide_id', 'congress'], 'unique': true},
      {'columns': ['bioguide_id', 'congress']},
      {'columns': ['congress', 'chamber']}
    ]
) }}

/*
   One current assignment edge per committee/member/Congress.  A Congress
   can have more than one directory edition; the newest official assignment
   observation wins deterministically.  Rows lacking either canonical ID
   remain in the staging seed and are reported as source-identity gaps rather
   than being guessed into the public relation.
*/
with ranked as (
    select
        assignments.committee_code,
        assignments.bioguide_id,
        assignments.congress,
        {{ normalize_chamber('assignments.chamber') }} as chamber,
        assignments.role,
        assignments.source_date,
        row_number() over (
            partition by assignments.committee_code, assignments.bioguide_id, assignments.congress
            order by assignments.source_date desc, assignments.source_granule_id desc,
                     assignments.committee_name, assignments.member_name
        ) as ordinal
    from {{ ref('stg_govinfo__congressional_directory_committee_assignments') }} as assignments
    join {{ ref('committees') }} as committees
      on committees.committee_code = assignments.committee_code
    join {{ ref('members') }} as members
      on members.bioguide_id = assignments.bioguide_id
    where assignments.committee_match_status = 'matched'
      and assignments.member_match_status = 'matched'
      and assignments.committee_code is not null
      and assignments.bioguide_id is not null
)
select
    committee_code,
    bioguide_id,
    congress,
    {{ normalize_chamber('chamber') }} as chamber,
    role
from ranked
where ordinal = 1
