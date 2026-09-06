{{ config(tags=["committee_reports", "staging"]) }}

with unnested as (
    select
        source_raw_id as parent_raw_id,
        source_endpoint_url as parent_endpoint_url,
        reports.report_json,
        committee.value as committee_json
    from {{ ref('int_congress_committee_report_records') }} as reports
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(reports.report_json -> 'committees') = 'array'
                then reports.report_json -> 'committees'
            else '[]'::jsonb
        end
    ) as committee(value)
)

select
    parent_raw_id,
    parent_endpoint_url,
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
    || {{ json_int('report_json', 'congress') }}::text as report_part_id,
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    {{ extract_fields('committee_json', [
        ('name', 'text'),
        ('systemCode', 'text', 'system_code'),
        ('url', 'text', 'committee_url')
    ]) }}
from unnested
