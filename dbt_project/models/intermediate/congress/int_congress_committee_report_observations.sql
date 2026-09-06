{{ config(materialized="table", tags=["committees", "committee_reports", "congress", "child_state"]) }}

/* Resolver-selected committee-report memberships with source lineage. */
with report_responses as (
    {{ resolve_child('committee_detail', 'committee_reports', 'committees') }}
), unnested as (
    select
        report_responses.raw_id as source_raw_id,
        report_responses.raw_response_version_id as source_raw_response_version_id,
        report_responses.latest_crawl_run_id as source_crawl_run_id,
        report_responses.parent_raw_id as committee_detail_raw_id,
        report_responses.parent_endpoint_url as committee_detail_endpoint_url,
        report_responses.endpoint_url as source_endpoint_url,
        report_responses.child_fetched_at as source_fetched_at,
        report_responses.page_offset as source_page_offset,
        report_responses.is_complete_generation as source_is_complete_generation,
        report_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        report_responses.generation_page_count as source_generation_page_count,
        report_responses.generation_expected_count as source_generation_expected_count,
        report_responses.generation_finished_at as source_generation_finished_at,
        reports.value as report_json,
        reports.array_ordinal as source_ordinal,
        committees.committee_id
    from report_responses
    join {{ ref('stg_congress__committee_detail') }} as committees
      on report_responses.parent_raw_id = committees.raw_id
    cross join lateral jsonb_array_elements(
        coalesce(report_responses.response -> 'reports', '[]'::jsonb)
    ) with ordinality as reports(value, array_ordinal)
), parsed as (
    select
        source_raw_id,
        source_raw_response_version_id,
        source_crawl_run_id,
        committee_detail_raw_id,
        committee_detail_endpoint_url,
        source_endpoint_url,
        source_fetched_at,
        source_page_offset,
        source_is_complete_generation,
        source_generation_crawl_run_id,
        source_generation_page_count,
        source_generation_expected_count,
        source_generation_finished_at,
        source_ordinal,
        committee_id,
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
)
select *
from parsed
