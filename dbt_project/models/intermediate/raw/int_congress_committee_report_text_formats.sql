{{ config(
    materialized="table",
    indexes=[{"columns": ["format_key"], "unique": true}, {"columns": ["report_part_id"]}]
) }}

/* Committee-report text is a paginated child collection. Resolve the latest
   complete baseline plus applicable non-empty deltas before expanding text
   and format arrays; selecting only the latest URL loses earlier windows. */
with text_responses as (
    {{ resolve_child('committee_report_detail', 'committee_report_texts', 'committee-reports') }}
),
text_items as (
    select
        text_responses.raw_id as text_raw_id,
        text_responses.endpoint_url as text_endpoint_url,
        text_responses.raw_response_version_id,
        text_responses.parent_raw_id as report_detail_raw_id,
        text_responses.child_fetched_at,
        text_responses.is_complete_generation as source_is_complete_generation,
        text_responses.response,
        reports.report_json,
        reports.source_raw_response_version_id as report_source_raw_response_version_id,
        text_item.ordinality::integer as text_item_ordinal,
        text_item.value as text_item_json
    from text_responses
    join {{ ref('int_congress_committee_report_records') }} as reports
      on reports.source_raw_id = text_responses.parent_raw_id
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(text_responses.response -> 'text') = 'array'
                then text_responses.response -> 'text'
            else '[]'::jsonb
        end
    ) with ordinality as text_item(value, ordinality)
),
formats as (
    select
        text_items.*,
        format_item.ordinality::integer as format_ordinal,
        format_item.value as format_json
    from text_items
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(text_items.text_item_json -> 'formats') = 'array'
                then text_items.text_item_json -> 'formats'
            else '[]'::jsonb
        end
    ) with ordinality as format_item(value, ordinality)
)

    select distinct on (text_raw_id, text_item_ordinal, format_ordinal)
    text_raw_id,
    text_endpoint_url,
    report_detail_raw_id,
    raw_response_version_id,
    child_fetched_at,
    source_is_complete_generation,
    report_source_raw_response_version_id,
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
    || case when {{ json_text('report_json', 'citation') }} ilike '%errata%' then 'e' else '' end
    || '-'
    || {{ json_int('report_json', 'congress') }}::text as report_part_id,
    text_item_ordinal,
    format_ordinal,
    text_raw_id::text || ':' || text_item_ordinal::text || ':' || format_ordinal::text as format_key,
    format_json
from formats
order by text_raw_id, text_item_ordinal, format_ordinal, report_part_id
