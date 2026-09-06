{{ config(materialized="table", tags=["bills", "congress", "child_state"]) }}

/* Resolver-selected subject occurrences, including nested legislativeSubjects. */
with subject_responses as (
    {{ resolve_child('bill_detail', 'bill_subjects', 'bills') }}
), unnested as (
    select
        subject_responses.raw_id as source_raw_id,
        subject_responses.raw_response_version_id as source_raw_response_version_id,
        subject_responses.latest_crawl_run_id as source_crawl_run_id,
        subject_responses.parent_raw_id as bill_detail_raw_id,
        subject_responses.parent_endpoint_url as bill_detail_endpoint_url,
        subject_responses.endpoint_url as source_endpoint_url,
        subject_responses.child_fetched_at as source_fetched_at,
        subject_responses.page_offset as source_page_offset,
        subject_responses.is_complete_generation as source_is_complete_generation,
        subject_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        subject_responses.generation_page_count as source_generation_page_count,
        subject_responses.generation_expected_count as source_generation_expected_count,
        subject_responses.generation_finished_at as source_generation_finished_at,
        subjects.value as subject_json,
        subjects.array_ordinal as source_ordinal,
        bills.bill_id
    from subject_responses
    join {{ ref('stg_congress__bill_detail') }} as bills
      on subject_responses.parent_raw_id = bills.raw_id
    cross join lateral jsonb_array_elements(
        coalesce(subject_responses.response #> '{subjects,legislativeSubjects}', '[]'::jsonb)
    ) with ordinality as subjects(value, array_ordinal)
)
select
    source_raw_id,
    source_raw_response_version_id,
    source_crawl_run_id,
    bill_detail_raw_id,
    bill_detail_endpoint_url,
    source_endpoint_url,
    source_fetched_at,
    source_page_offset,
    source_is_complete_generation,
    source_generation_crawl_run_id,
    source_generation_page_count,
    source_generation_expected_count,
    source_generation_finished_at,
    source_ordinal,
    bill_id,
    nullif(subject_json #>> '{name}', '') as subject,
    (nullif(subject_json #>> '{updateDate}', ''))::timestamptz as updated_at
from unnested
