{{ config(tags=["treaties", "staging"]) }}

with detail_responses as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'treaties'
      and node_type = 'treaty_detail'
),
unnested as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        treaty_item.value as treaty_json
    from detail_responses
    cross join lateral {{ unnest_array('response', 'treaty', 'treaty_item') }}
),
typed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        'td'
        || {{ json_int('treaty_json', 'congressReceived') }}::text
        || '-'
        || {{ json_int('treaty_json', 'number') }}::text as treaty_id,
        'td'
        || {{ json_int('treaty_json', 'congressReceived') }}::text
        || '-'
        || {{ json_int('treaty_json', 'number') }}::text
        || coalesce({{ json_text('treaty_json', 'suffix') }}, '') as treaty_part_id,
        {{ json_int('treaty_json', 'number') }} as treaty_number,
        {{ json_text('treaty_json', 'suffix') }} as suffix,
        {{ json_int('treaty_json', 'congressReceived') }} as congress_received,
        {{ json_int('treaty_json', 'congressConsidered') }} as congress_considered,
        {{ json_text('treaty_json', 'topic') }} as topic,
        {{ json_timestamptz('treaty_json', 'transmittedDate') }} as transmitted_at,
        {{ json_date('treaty_json', 'inForceDate') }} as in_force_at,
        {{ json_text('treaty_json', 'resolutionText') }} as resolution_text,
        coalesce({{ json_int('treaty_json', 'parts.count') }}, 0) as parts_count,
        coalesce({{ json_int('treaty_json', 'actions.count') }}, 0) as actions_count,
        {{ json_text('treaty_json', 'oldNumber') }} as old_number,
        {{ json_text('treaty_json', 'oldNumberDisplayName') }} as old_number_display_name,
        {{ json_timestamptz('treaty_json', 'updateDate') }} as updated_at
    from unnested
),
deduped as (
    select
        *,
        row_number() over (
            partition by treaty_part_id
            order by updated_at desc nulls last, congress_considered desc nulls last, actions_count desc nulls last
        ) as _rn
    from typed
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    treaty_id,
    treaty_part_id,
    treaty_number,
    suffix,
    congress_received,
    congress_considered,
    topic,
    transmitted_at,
    in_force_at,
    resolution_text,
    parts_count,
    actions_count,
    old_number,
    old_number_display_name,
    updated_at
from deduped
where _rn = 1
