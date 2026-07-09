{{ config(tags=["committees", "staging"]) }}

with report_responses as (
    {{ resolve_child('committee_detail', 'committee_reports', 'committees') }}
),
unnested as (
    select
        parent_raw_id,
        report.value as report_json
    from report_responses
    cross join lateral {{ unnest_array('response', 'reports', 'report') }}
),
committee_lookup as (
    select
        id as raw_id,
        response -> 'committee' as committee_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'committees'
      and node_type = 'committee_detail'
)

select
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    {{ build_custom_id([
        "lower(" ~ json_text('report_json', 'type') ~ ")",
        "(" ~ json_text('report_json', 'number') ~ ")::text"
    ]) }} || '-' || {{ json_int('report_json', 'congress') }}::text as report_id,
    {{ build_custom_id([
        "lower(" ~ json_text('report_json', 'type') ~ ")",
        "(" ~ json_text('report_json', 'number') ~ ")::text"
    ]) }}
    || '-'
    || coalesce(({{ json_int('report_json', 'part') }})::text, '1')
    || case
        when {{ json_text('report_json', 'citation') }} ilike '%errata%' then 'e'
        else ''
       end
    || '-'
    || {{ json_int('report_json', 'congress') }}::text as report_part_id
from unnested
inner join committee_lookup
    on unnested.parent_raw_id = committee_lookup.raw_id
