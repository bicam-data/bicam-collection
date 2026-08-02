{{ config(tags=["hearings", "staging"]) }}

with detail as (
    select
        id as raw_id,
        endpoint_url,
        raw_response_version_id,
        response -> 'hearing' as hearing_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'hearings'
      and node_type = 'hearing_detail'
),
base as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        hearing_json,
        lower({{ json_text('hearing_json', 'chamber') }}) as chamber,
        coalesce({{ json_text('hearing_json', 'part') }}, '0') as hearing_part
    from detail
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
    {{ json_bigint('hearing_json', 'jacketNumber') }} as hearing_jacketnumber,
    hearing_part,
    {{ json_int('hearing_json', 'congress') }} as congress,
    {{ json_text('hearing_json', 'number') }} as hearing_number,
    {{ json_text('hearing_json', 'libraryOfCongressIdentifier') }} as loc_id,
    {{ json_text('hearing_json', 'citation') }} as citation,
    {{ json_text('hearing_json', 'associatedMeeting.eventId') }} as associated_meeting_id,
    {{ json_timestamptz('hearing_json', 'updateDate') }} as updated_at
from base
