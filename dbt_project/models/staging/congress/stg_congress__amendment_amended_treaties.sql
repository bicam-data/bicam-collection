{{ config(tags=["amendments", "staging"]) }}

with amendment_detail as (
    select
        detail.raw_id,
        detail.amendment_id,
        raw.response -> 'amendment' as amendment_json
    from {{ ref('stg_congress__amendment_detail') }} as detail
    join {{ ref('int_raw_responses__current') }} as raw
      on detail.raw_id = raw.raw_id
    where coalesce(raw.response #>> '{amendment,amendedTreaty,treatyNumber}', '') <> ''
)

select
    amendment_id,
    'td'
    || coalesce({{ json_int('amendment_json', 'amendedTreaty.congress') }}::text, '')
    || '-'
    || coalesce({{ json_text('amendment_json', 'amendedTreaty.treatyNumber') }}, '') as treaty_id,
    {{ json_int('amendment_json', 'amendedTreaty.congress') }} as congress,
    {{ json_text('amendment_json', 'amendedTreaty.treatyNumber') }} as number
from amendment_detail
