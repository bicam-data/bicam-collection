{{ config(tags=["hearings", "staging"]) }}

with detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        fetched_at,
        response -> 'hearing' as hearing_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'hearings'
      and node_type = 'hearing_detail'
),
base as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        fetched_at,
        hearing_json,
        lower({{ json_text('hearing_json', 'chamber') }}) as source_chamber,
        {{ congress_hearing_canonical_chamber('hearing_json') }} as chamber,
        coalesce({{ json_text('hearing_json', 'part') }}, '0') as hearing_part
    from detail
),
identified as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        fetched_at,
        hearing_json,
        lower(left(chamber, 1))
        || 'hrg'
        || {{ json_bigint('hearing_json', 'jacketNumber') }}::text
        || '-'
        || {{ json_int('hearing_json', 'congress') }}::text as hearing_id,
        lower(left(chamber, 1))
        || 'hrg'
        || {{ json_bigint('hearing_json', 'jacketNumber') }}::text
        || '-'
        || hearing_part
        || '-'
        || {{ json_int('hearing_json', 'congress') }}::text as hearing_part_id,
        chamber,
        source_chamber,
        hearing_part
    from base
),
ranked as (
    select
        identified.*,
        row_number() over (
            partition by hearing_id, hearing_part_id
            order by
                case
                    when source_chamber = 'joint' then 0
                    when {{ json_text('hearing_json', 'citation') }} ~* '^J[.]Hrg[.]' then 1
                    else 2
                end,
                fetched_at desc,
                endpoint_url,
                raw_id
        ) as _identity_rank
    from identified
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    lower(left(chamber, 1))
    || 'hrg'
    || {{ json_bigint('hearing_json', 'jacketNumber') }}::text
    || '-'
    || {{ json_int('hearing_json', 'congress') }}::text as hearing_id,
    lower(left(chamber, 1))
    || 'hrg'
    || {{ json_bigint('hearing_json', 'jacketNumber') }}::text
    || '-'
    || hearing_part
    || '-'
    || {{ json_int('hearing_json', 'congress') }}::text as hearing_part_id,
    chamber,
    source_chamber,
    {{ json_bigint('hearing_json', 'jacketNumber') }} as hearing_jacketnumber,
    hearing_part,
    {{ json_int('hearing_json', 'congress') }} as congress,
    {{ json_text('hearing_json', 'number') }} as hearing_number,
    {{ json_text('hearing_json', 'libraryOfCongressIdentifier') }} as loc_id,
    {{ json_text('hearing_json', 'citation') }} as citation,
    {{ json_text('hearing_json', 'associatedMeeting.eventId') }} as associated_meeting_id,
    {{ json_timestamptz('hearing_json', 'updateDate') }} as updated_at
from ranked
where _identity_rank = 1
