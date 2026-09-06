{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'hearings'],
    indexes=[
      {'columns': ['hearing_id'], 'unique': true},
      {'columns': ['congress', 'chamber']},
      {'columns': ['hearing_jacketnumber', 'congress', 'chamber']}
    ]
) }}

/*
  Public hearings are a canonical union, not merely the Congress.gov side of
  reconciliation. Congress.gov is preferred when both sources identify the
  same hearing; GovInfo contributes historical/source-gap hearings. A small
  number of joint hearing identities exist only in committee-meeting child
  payloads, so reciprocal relationship evidence supplies those roots too.
*/
with govinfo_matches as (
    select
        govinfo_source_record_id as package_id,
        congress_source_record_id as source_hearing_id,
        govinfo_title,
        row_number() over (
            partition by congress_source_record_id
            order by govinfo_source_record_id
        ) as _match_rank
    from {{ ref('fct_hearing_matches') }}
    where congress_source_record_id is not null
),
congress_current as (
    select
        public_ids.public_hearing_id as hearing_id,
        substring(public_ids.public_hearing_id from '^[hsj]hrg([0-9]+)') as hearing_jacketnumber,
        h.loc_id,
        coalesce(
            nullif(raw.response #>> '{hearing,title}', ''),
            matches.govinfo_title,
            h.citation
        ) as title,
        h.congress::integer as congress,
        {{ json_int('raw.response', 'hearing.session') }} as session,
        case substring(public_ids.public_hearing_id from '^([hsj])hrg')
            when 'h' then 'house'
            when 's' then 'senate'
            when 'j' then 'joint'
        end as chamber,
        case
            when {{ json_boolean('raw.response', 'hearing.isAppropriation') }} is not null
                then {{ json_boolean('raw.response', 'hearing.isAppropriation') }}
            when nullif(raw.response #>> '{hearing,title}', '') is null then null::boolean
            else (raw.response #>> '{hearing,title}') ilike '%appropriation%'
        end as is_appropriation,
        h.hearing_number,
        h.hearing_part as part_number,
        h.citation,
        {{ json_int('raw.response', 'hearing.pages') }} as pages,
        h.updated_at::timestamptz as updated_at,
        row_number() over (
            partition by public_ids.public_hearing_id
            order by
                case
                    when public_ids.public_hearing_id ~ '^hhrg' and h.chamber = 'house' then 0
                    when public_ids.public_hearing_id ~ '^shrg' and h.chamber = 'senate' then 0
                    when public_ids.public_hearing_id ~ '^jhrg' and h.chamber = 'joint' then 0
                    else 1
                end,
                h.updated_at desc nulls last,
                h.raw_id desc
        ) as _rank
    from {{ ref('int_hearings__congress') }} as h
    join {{ ref('int_congress_hearing_public_id_map') }} as public_ids
      on public_ids.source_hearing_id = h.hearing_id
    left join {{ ref('int_raw_responses__current') }} as raw
      on raw.raw_id = h.raw_id
    left join govinfo_matches as matches
      on matches.source_hearing_id = h.hearing_id
     and matches._match_rank = 1
),
congress_rows as (
    select
        hearing_id, hearing_jacketnumber, loc_id, title, congress, session,
        chamber, is_appropriation, hearing_number, part_number, citation,
        pages, updated_at
    from congress_current
    where _rank = 1
),
govinfo_parsed as (
    select
        packages.package_id,
        regexp_match(
            packages.package_id,
            '^CHRG-([0-9]+)([hsj]hrg)([0-9]+)(.*)$',
            'i'
        ) as package_parts,
        packages.title,
        packages.congress,
        packages.last_modified,
        raw.response
    from {{ ref('stg_govinfo__hearings_packages') }} as packages
    join {{ ref('int_raw_responses__current') }} as raw
      on raw.raw_id = packages.raw_id
    where packages.package_id ~* '^CHRG-[0-9]+[hsj]hrg[0-9]+'
),
govinfo_identified as (
    select
        {{ bicam_govinfo_hearing_id('package_id') }} as hearing_id,
        coalesce(nullif(ltrim(package_parts[3], '0'), ''), '0') as hearing_jacketnumber,
        package_parts,
        package_id,
        title,
        congress::integer as congress,
        case package_parts[2]
            when 'hhrg' then 'house'
            when 'shrg' then 'senate'
            when 'jhrg' then 'joint'
        end as chamber,
        last_modified,
        response
    from govinfo_parsed
    where package_parts is not null
),
govinfo_ranked as (
    select
        identified.*,
        row_number() over (
            partition by hearing_id
            order by
                last_modified desc nulls last,
                package_id
        ) as _rank
    from govinfo_identified as identified
    where not exists (
        select 1
        from congress_rows
        where congress_rows.hearing_id = identified.hearing_id
    )
),
govinfo_rows as (
    select
        hearing_id,
        hearing_jacketnumber,
        null::text as loc_id,
        title,
        congress,
        nullif(response ->> 'session', '')::integer as session,
        chamber,
        case
            when title is null then null::boolean
            else title ilike '%appropriation%'
        end as is_appropriation,
        null::text as hearing_number,
        nullif(package_parts[4], '') as part_number,
        null::text as citation,
        nullif(response ->> 'pages', '')::integer as pages,
        last_modified::timestamptz as updated_at
    from govinfo_ranked
    where _rank = 1
),
known_rows as (
    select * from congress_rows
    union all
    select * from govinfo_rows
),
meeting_only_ranked as (
    select
        bridge.canonical_hearing_id as hearing_id,
        substring(bridge.canonical_hearing_id from 'hrg([0-9]+)-') as hearing_jacketnumber,
        meetings.title,
        bridge.congress::integer as congress,
        bridge.chamber,
        meetings.updated_at,
        row_number() over (
            partition by bridge.canonical_hearing_id
            order by meetings.updated_at desc nulls last, bridge.committee_meeting_id
        ) as _rank
    from {{ ref('bridge_hearings__committee_meetings') }} as bridge
    join {{ ref('int_committee_meetings__congress') }} as meetings
      on meetings.meeting_id = bridge.committee_meeting_id
    where bridge.canonical_hearing_id is not null
      and not exists (
        select 1
        from known_rows
        where known_rows.hearing_id = bridge.canonical_hearing_id
    )
),
meeting_only_rows as (
    select
        hearing_id,
        hearing_jacketnumber,
        null::text as loc_id,
        title,
        congress,
        null::integer as session,
        chamber,
        case
            when title is null then null::boolean
            else title ilike '%appropriation%'
        end as is_appropriation,
        null::text as hearing_number,
        null::text as part_number,
        null::text as citation,
        null::integer as pages,
        updated_at::timestamptz as updated_at
    from meeting_only_ranked
    where _rank = 1
)

select * from known_rows
union all
select * from meeting_only_rows
