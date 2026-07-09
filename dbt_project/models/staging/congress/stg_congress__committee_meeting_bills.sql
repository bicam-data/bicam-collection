{{ config(tags=["committee_meetings", "staging"]) }}

with detail as (
    select
        response -> 'committeeMeeting' as meeting_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'committee-meetings'
      and node_type = 'committee_meeting_detail'
),
unnested as (
    select
        {{ json_text('meeting_json', 'eventId') }}
        || '-'
        || {{ json_int('meeting_json', 'congress') }}::text as meeting_id,
        bill.value as bill_json
    from detail
    cross join lateral {{ unnest_array('meeting_json', 'relatedItems.bills', 'bill') }}
)

select
    meeting_id,
    lower({{ json_text('bill_json', 'type') }})
    || {{ json_text('bill_json', 'number') }}
    || '-'
    || {{ json_int('bill_json', 'congress') }}::text as bill_id,
    lower({{ json_text('bill_json', 'type') }}) as bill_type,
    {{ json_text('bill_json', 'number') }} as bill_number,
    {{ json_int('bill_json', 'congress') }} as congress
from unnested
