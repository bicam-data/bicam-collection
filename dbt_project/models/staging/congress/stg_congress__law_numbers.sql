{{ config(tags=["laws", "staging"]) }}

with detail as (
    select
        raw_id,
        coalesce(response -> 'bill', response) as bill_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'laws'
      and node_type = 'law_detail'
), unnested as (
    select
        raw_id,
        bill_json,
        law_item.value as law_item
    from detail
    cross join lateral {{ unnest_array('bill_json', 'laws', 'law_item') }}
)
select
    raw_id,
    lower({{ json_text('bill_json', 'type') }})
    || {{ congress_bill_number('bill_json') }}
    || '-' || {{ json_int('bill_json', 'congress') }}::text as bill_id,
    lower({{ json_text('law_item', 'type') }}) as law_type,
    {{ json_text('law_item', 'number') }} as law_number
from unnested
