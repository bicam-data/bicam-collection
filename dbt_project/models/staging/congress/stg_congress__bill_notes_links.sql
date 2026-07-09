{{ config(tags=["bills", "staging"]) }}

with detail as (
    select
        bill_detail.bill_id,
        raw_responses.response -> 'bill' as bill_json
    from {{ source('raw', 'raw_responses') }} as raw_responses
    join {{ ref('stg_congress__bill_detail') }} as bill_detail
      on raw_responses.id = bill_detail.raw_id
    where raw_responses.source = 'congress'
      and raw_responses.category = 'bills'
      and raw_responses.node_type = 'bill_detail'
),
unnested_notes as (
    select
        bill_id,
        note_item.value as note_json
    from detail
    cross join lateral jsonb_array_elements(
        coalesce(detail.bill_json #> '{notes}', '[]'::jsonb)
    ) with ordinality as note_item(value, note_ordinality)
),
unnested_links as (
    select
        bill_id,
        note_json,
        link_item.value as link_json
    from unnested_notes
    cross join lateral jsonb_array_elements(
        coalesce(unnested_notes.note_json #> '{links}', '[]'::jsonb)
    ) as link_item(value)
)

select
    bill_id,
    {{ hash_to_uuid("bill_id || '|' || coalesce(note_json::text, '')") }} as note_id,
    nullif(link_json #>> '{name}', '') as name,
    nullif(link_json #>> '{url}', '') as url
from unnested_links
