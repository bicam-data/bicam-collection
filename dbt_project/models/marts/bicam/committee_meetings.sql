{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committee_meetings'],
    indexes=[
      {'columns': ['meeting_id'], 'unique': true},
      {'columns': ['congress', 'chamber']},
      {'columns': ['date']}
    ]
) }}

with ranked as (
    select
        meetings.*,
        row_number() over (
            partition by meeting_id
            order by updated_at desc nulls last, source_raw_id desc
        ) as _rn
    from {{ ref('int_committee_meetings__congress') }} as meetings
),
current_rows as (
select
    meeting_id,
    title,
    meeting_type,
    chamber,
    congress,
    date,
    room,
    street_address,
    building,
    city,
    state,
    zip_code,
    meeting_status,
    updated_at
from ranked
where _rn = 1
),
relationship_only_ranked as (
    select
        bridge.committee_meeting_id as meeting_id,
        coalesce(
            nullif(raw.response #>> '{hearing,title}', ''),
            hearing.citation
        ) as title,
        'hearing'::text as meeting_type,
        hearing.chamber,
        hearing.congress::integer as congress,
        dates.hearing_date::date as date,
        null::text as room,
        null::text as street_address,
        null::text as building,
        null::text as city,
        null::text as state,
        null::text as zip_code,
        null::text as meeting_status,
        hearing.updated_at::timestamptz as updated_at,
        row_number() over (
            partition by {{ bicam_public_committee_meeting_id('bridge.committee_meeting_id') }}
            order by hearing.updated_at desc nulls last, dates.hearing_date, hearing.hearing_id
        ) as _rn
    from {{ ref('bridge_hearings__committee_meetings') }} as bridge
    join {{ ref('int_hearings__congress') }} as hearing
      on hearing.hearing_id = bridge.canonical_hearing_id
    left join {{ ref('int_raw_responses__current') }} as raw
      on raw.raw_id = hearing.raw_id
    left join {{ ref('stg_congress__hearing_dates') }} as dates
      on dates.hearing_id = hearing.hearing_id
    where not exists (
        select 1
        from current_rows
        where {{ bicam_public_committee_meeting_id('current_rows.meeting_id') }}
            = {{ bicam_public_committee_meeting_id('bridge.committee_meeting_id') }}
    )
),
relationship_only_rows as (
    select
        meeting_id, title, meeting_type, chamber, congress, date, room,
        street_address, building, city, state, zip_code, meeting_status,
        updated_at
    from relationship_only_ranked
    where _rn = 1
),
combined as (
    select * from current_rows
    union all
    select * from relationship_only_rows
)

select
    {{ bicam_public_committee_meeting_id('meeting_id') }} as meeting_id,
    title,
    meeting_type,
    chamber,
    congress,
    date,
    room,
    street_address,
    building,
    city,
    state,
    zip_code,
    meeting_status,
    updated_at
from combined
