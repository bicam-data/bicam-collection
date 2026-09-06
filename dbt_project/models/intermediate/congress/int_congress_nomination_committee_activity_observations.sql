{{ config(materialized="table", tags=["nominations", "congress", "child_state"]) }}

/* Resolver-selected nomination committee/activity observations with source
   lineage. */
with committee_responses as (
    {{ resolve_child('nomination_detail', 'nomination_committees', 'nominations') }}
), unnested_committees as (
    select
        committee_responses.raw_id as source_raw_id,
        committee_responses.raw_response_version_id as source_raw_response_version_id,
        committee_responses.latest_crawl_run_id as source_crawl_run_id,
        committee_responses.parent_raw_id as nomination_detail_raw_id,
        committee_responses.parent_endpoint_url as nomination_detail_endpoint_url,
        committee_responses.endpoint_url as source_endpoint_url,
        committee_responses.child_fetched_at as source_fetched_at,
        committee_responses.page_offset as source_page_offset,
        committee_responses.is_complete_generation as source_is_complete_generation,
        committee_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        committee_responses.generation_page_count as source_generation_page_count,
        committee_responses.generation_expected_count as source_generation_expected_count,
        committee_responses.generation_finished_at as source_generation_finished_at,
        substring(committee_responses.parent_endpoint_url from '/nomination/([0-9]+)/')::integer as congress,
        substring(committee_responses.parent_endpoint_url from '/nomination/[0-9]+/([0-9]+)') as nomination_number,
        lpad(coalesce(
            substring(committee_responses.parent_endpoint_url from '/nomination/[0-9]+/[0-9]+-([0-9]+)'),
            '00'
        ), 2, '0') as nomination_part,
        committee.value as committee_json,
        committee.array_ordinal as source_ordinal
    from committee_responses
    cross join lateral jsonb_array_elements(
        coalesce(committee_responses.response -> 'committees', '[]'::jsonb)
    ) with ordinality as committee(value, array_ordinal)
), unnested_activities as (
    select
        unnested_committees.*,
        activity.value as activity_json,
        activity.array_ordinal as activity_ordinal
    from unnested_committees
    cross join lateral jsonb_array_elements(
        coalesce(committee_json -> 'activities', '[]'::jsonb)
    ) with ordinality as activity(value, array_ordinal)
)
select
    source_raw_id,
    source_raw_response_version_id,
    source_crawl_run_id,
    nomination_detail_raw_id,
    nomination_detail_endpoint_url,
    source_endpoint_url,
    source_fetched_at,
    source_page_offset,
    source_is_complete_generation,
    source_generation_crawl_run_id,
    source_generation_page_count,
    source_generation_expected_count,
    source_generation_finished_at,
    source_ordinal,
    activity_ordinal,
    'pn' || nomination_number || '-' || congress::text as nomination_id,
    'pn' || nomination_number || '-' || nomination_part || '-' || congress::text as nomination_part_id,
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    {{ json_text('committee_json', 'name') }} as committee_name,
    {{ json_text('committee_json', 'type') }} as committee_type,
    case
        when jsonb_typeof(activity_json) = 'string'
            then nullif(trim(both '"' from activity_json::text), '')
        else nullif(activity_json ->> 'name', '')
    end as activity_name,
    case
        when jsonb_typeof(activity_json) = 'object'
            then nullif(activity_json ->> 'date', '')::date
        else null
    end as activity_date
from unnested_activities
