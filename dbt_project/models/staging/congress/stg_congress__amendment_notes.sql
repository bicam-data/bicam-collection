{{ config(tags=["amendments", "staging"]) }}

with detail as (
    select
        amendment_detail.amendment_id,
        raw_responses.response -> 'amendment' as amendment_json
    from {{ ref('int_raw_responses__current') }} as raw_responses
    join {{ ref('stg_congress__amendment_detail') }} as amendment_detail
      on raw_responses.raw_id = amendment_detail.raw_id
    where raw_responses.source = 'congress'
      and raw_responses.category = 'amendments'
      and raw_responses.node_type = 'amendment_detail'
),
unnested as (
    select
        amendment_id,
        note_item.value as note_json
    from detail
    cross join lateral jsonb_array_elements(
        coalesce(detail.amendment_json #> '{notes}', '[]'::jsonb)
    ) with ordinality as note_item(value, note_ordinality)
)

select
    amendment_id,
    {{ hash_to_uuid("amendment_id || '|' || coalesce(note_json::text, '')") }} as note_id,
    {{ json_text('note_json', 'text') }} as text
from unnested
