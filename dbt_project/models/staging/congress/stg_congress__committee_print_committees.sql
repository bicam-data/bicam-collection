{{ config(tags=["committee_prints", "staging"]) }}

with detail as (
    select
        response
    from {{ source('raw', 'raw_responses') }}
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
        committee.value as committee_json
    from prints
    cross join lateral {{ unnest_array('print_json', 'committees', 'committee') }}
)

select
    print_id,
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    {{ json_text('committee_json', 'name') }} as committee_name
from unnested
