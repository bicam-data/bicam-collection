{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'members'],
    indexes=[
      {'columns': ['bioguide_id', 'congress', 'chamber', 'start_year'], 'unique': true},
      {'columns': ['congress', 'chamber']},
      {'columns': ['state_code']}
    ]
) }}

with congress_terms as (
    select distinct
        bioguide_id,
        member_type,
        chamber,
        congress,
        start_year,
        end_year,
        state_name,
        state_code,
        district::text as district
    from {{ ref('stg_congress__member_terms') }}
    where bioguide_id is not null
), directory_terms as (
    /* A directory can have several editions for the same Congress. Prefer
       the newest non-null observation independently for each field: later
       editions occasionally omit a URL or biography that an earlier edition
       supplied, and the public term must preserve that source evidence. */
    select
        bridge.linkable_bioguide_id as bioguide_id,
        directory_members.congress,
        case
            when upper(directory_members.chamber) = 'H' then 'house of representatives'
            when upper(directory_members.chamber) = 'S' then 'senate'
            else lower(directory_members.chamber)
        end as chamber,
        (array_agg(
            directory_members.granule_title
            order by directory_members.source_issued_at desc nulls last,
                     directory_members.directory_last_modified desc nulls last,
                     directory_members.granule_id desc
        ) filter (where directory_members.granule_title is not null))[1] as title,
        (array_agg(
            directory_members.biography
            order by directory_members.source_issued_at desc nulls last,
                     directory_members.directory_last_modified desc nulls last,
                     directory_members.granule_id desc
        ) filter (where directory_members.biography is not null))[1] as biography,
        (array_agg(
            directory_members.population
            order by directory_members.source_issued_at desc nulls last,
                     directory_members.directory_last_modified desc nulls last,
                     directory_members.granule_id desc
        ) filter (where directory_members.population is not null))[1] as population,
        (array_agg(
            directory_members.online
            order by directory_members.source_issued_at desc nulls last,
                     directory_members.directory_last_modified desc nulls last,
                     directory_members.granule_id desc
        ) filter (where directory_members.online is not null))[1] as other_url
    from {{ ref('stg_govinfo__congressional_directory_members') }} as directory_members
    left join {{ ref('bridge_govinfo__member_references') }} as bridge
      on bridge.source_member_reference_key = directory_members.source_member_reference_key
    where bridge.linkable_bioguide_id is not null
      and directory_members.congress is not null
    group by
        bridge.linkable_bioguide_id,
        directory_members.congress,
        directory_members.chamber
)
select
    congress_terms.bioguide_id,
    congress_terms.member_type,
    congress_terms.chamber,
    congress_terms.congress,
    congress_terms.start_year,
    congress_terms.end_year,
    congress_terms.state_name,
    congress_terms.state_code,
    congress_terms.district,
    directory_terms.title,
    directory_terms.biography,
    directory_terms.population,
    null::text as twitter_url,
    null::text as instagram_url,
    null::text as facebook_url,
    null::text as youtube_url,
    directory_terms.other_url
from congress_terms
left join directory_terms
  on directory_terms.bioguide_id = congress_terms.bioguide_id
 and directory_terms.congress = congress_terms.congress
 and directory_terms.chamber = congress_terms.chamber
