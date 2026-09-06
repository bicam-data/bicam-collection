{{ config(tags=["bills", "staging"]) }}

with detail as (
    select
        bill_detail.bill_id,
        raw_responses.response -> 'bill' as bill_json
    from {{ ref('int_raw_responses__current') }} as raw_responses
    join {{ ref('stg_congress__bill_detail') }} as bill_detail
      on raw_responses.raw_id = bill_detail.raw_id
    where raw_responses.source = 'congress'
      and raw_responses.category = 'bills'
      and raw_responses.node_type = 'bill_detail'
),
unnested as (
    select
        bill_id,
        note_item.value as note_json
    from detail
    cross join lateral jsonb_array_elements(
        coalesce(detail.bill_json #> '{notes}', '[]'::jsonb)
    ) with ordinality as note_item(value, note_ordinality)
)

select
    bill_id,
    {{ hash_to_uuid("bill_id || '|' || coalesce(note_json::text, '')") }} as note_id,
    nullif(note_json #>> '{text}', '') as note_text
from unnested
