{{ config(tags=["amendments", "staging"]) }}

with amendment_detail as (
    select
        detail.raw_id,
        detail.amendment_id,
        raw.response -> 'amendment' as amendment_json
    from {{ ref('stg_congress__amendment_detail') }} as detail
    join {{ source('raw', 'raw_responses') }} as raw
      on detail.raw_id = raw.id
    where coalesce(raw.response #>> '{amendment,amendedAmendment,number}', '') <> ''
)

select
    amendment_detail.amendment_id,
    {{ build_custom_id([
        "lower(" ~ json_text('amendment_json', 'amendedAmendment.type') ~ ")",
        json_text('amendment_json', 'amendedAmendment.number')
    ]) }} || '-' || coalesce({{ json_int('amendment_json', 'amendedAmendment.congress') }}::text, '') as amended_amendment_id,
    lower({{ json_text('amendment_json', 'amendedAmendment.type') }}) as amendment_type,
    {{ json_text('amendment_json', 'amendedAmendment.number') }} as amendment_number,
    {{ json_int('amendment_json', 'amendedAmendment.congress') }} as congress
from amendment_detail
