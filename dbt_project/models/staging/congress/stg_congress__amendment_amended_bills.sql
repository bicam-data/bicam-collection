{{ config(tags=["amendments", "staging"]) }}

with amendment_detail as (
    select
        detail.raw_id,
        detail.amendment_id,
        raw.response -> 'amendment' as amendment_json
    from {{ ref('stg_congress__amendment_detail') }} as detail
    join {{ source('raw', 'raw_responses') }} as raw
      on detail.raw_id = raw.id
    where coalesce(raw.response #>> '{amendment,amendedBill,number}', '') <> ''
)

select
    amendment_id,
    {{ build_custom_id([
        "lower(" ~ json_text('amendment_json', 'amendedBill.type') ~ ")",
        json_text('amendment_json', 'amendedBill.number')
    ]) }} || '-' || coalesce({{ json_int('amendment_json', 'amendedBill.congress') }}::text, '') as bill_id,
    lower({{ json_text('amendment_json', 'amendedBill.type') }}) as bill_type,
    {{ json_text('amendment_json', 'amendedBill.number') }} as bill_number,
    {{ json_int('amendment_json', 'amendedBill.congress') }} as congress
from amendment_detail
