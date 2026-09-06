{{ config(tags=["bound_congressional_record", "staging"]) }}

with source_rows as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'bound-congressional-record'
      and node_type = 'bound_congressional_record_detail'
),
unnested as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        record.value as record_json
    from source_rows
    cross join lateral {{ unnest_array('response', 'boundCongressionalRecord', 'record') }}
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    {{ json_text('record_json', 'volumeNumber') }} || '-' || {{ json_text('record_json', 'date') }} as bound_record_id,
    {{ json_int('record_json', 'volumeNumber') }} as volume_number,
    {{ json_date('record_json', 'date') }} as record_date,
    {{ json_int('record_json', 'congress') }} as congress,
    {{ json_int('record_json', 'sessionNumber') }} as session_number,
    {{ json_date('record_json', 'updateDate') }} as updated_at,
    {{ json_text('record_json', 'url') }} as record_url,
    record_json -> 'dailyDigest' as daily_digest,
    record_json -> 'sections' as sections
from unnested
