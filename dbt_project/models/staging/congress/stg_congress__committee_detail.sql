{{ config(tags=["committees", "staging"]) }}

with committee_list_raw as (
    select
        raw.id as raw_id,
        raw.response as response_json
    from {{ source('raw', 'raw_responses') }} as raw
    where raw.source = 'congress'
      and raw.category = 'committees'
      and raw.node_type = 'committee_list'
),
committee_list as (
    select
        committee.value as committee_json
    from committee_list_raw
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(response_json -> 'committees') = 'array' then response_json -> 'committees'
            else '[]'::jsonb
        end
    ) as committee(value)
),
committee_list_latest as (
    select distinct on ({{ json_text('committee_json', 'systemCode') }})
        {{ json_text('committee_json', 'systemCode') }} as committee_id,
        {{ json_text('committee_json', 'name') }} as name,
        lower({{ json_text('committee_json', 'chamber') }}) as chamber,
        {{ json_timestamptz('committee_json', 'updateDate') }} as list_updated_at
    from committee_list
    order by
        {{ json_text('committee_json', 'systemCode') }},
        {{ json_timestamptz('committee_json', 'updateDate') }} desc nulls last
),
committee_detail as (
    select
        raw.response -> 'committee' as committee_json
    from {{ source('raw', 'raw_responses') }} as raw
    where raw.source = 'congress'
      and raw.category = 'committees'
      and raw.node_type = 'committee_detail'
)

select
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    regexp_replace({{ json_text('committee_json', 'systemCode') }}, '[0-9]', '', 'g') as system_code,
    nullif(regexp_replace({{ json_text('committee_json', 'systemCode') }}, '[^0-9]', '', 'g'), '') as system_number,
    committee_list_latest.name,
    committee_list_latest.chamber,
    {{ json_text('committee_json', 'type') }} as committee_type,
    {{ json_boolean('committee_json', 'isCurrent', false) }} as is_current,
    {{ json_text('committee_json', 'committeeWebsiteUrl') }} as committee_url,
    {{ json_int('committee_json', 'bills,count', 0) }} as bills_count,
    {{ json_int('committee_json', 'reports,count', 0) }} as reports_count,
    {{ json_int('committee_json', 'nominations,count', 0) }} as nominations_count,
    {{ json_timestamptz('committee_json', 'updateDate') }} as updated_at
from committee_detail
left join committee_list_latest
    on {{ json_text('committee_json', 'systemCode') }} = committee_list_latest.committee_id
