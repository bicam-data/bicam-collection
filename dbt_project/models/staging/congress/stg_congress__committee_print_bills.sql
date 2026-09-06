{{ config(tags=["committee_prints", "staging"]) }}

with detail as (
    select
        response
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'committee-prints'
      and node_type = 'committee_print_detail'
),
prints as (
    select
        print.value as print_json
    from detail
    cross join lateral {{ unnest_array('response', 'committeePrint', 'print') }}
),
unnested as (
    select
        lower(left({{ json_text('print_json', 'chamber') }}, 1))
        || 'prt'
        || {{ json_int('print_json', 'jacketNumber') }}::text
        || '-'
        || {{ json_int('print_json', 'congress') }}::text as print_id,
        bill.value as bill_json
    from prints
    cross join lateral {{ unnest_array('print_json', 'associatedBills', 'bill') }}
)

select
    print_id,
    lower({{ json_text('bill_json', 'type') }})
    || {{ normalize_congress_bill_number_text(json_text('bill_json', 'number')) }}
    || '-'
    || {{ json_int('bill_json', 'congress') }}::text as bill_id,
    lower({{ json_text('bill_json', 'type') }}) as bill_type,
    {{ normalize_congress_bill_number_text(json_text('bill_json', 'number')) }} as bill_number,
    {{ json_int('bill_json', 'congress') }} as congress
from unnested
