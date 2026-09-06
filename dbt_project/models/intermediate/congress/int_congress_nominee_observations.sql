{{ config(tags=["nominations", "congress"]) }}

/*
   Keep the source observation identity needed to reconcile repeated
   nomination child snapshots. The public relation intentionally does not
   expose these fields, but it must distinguish two people sharing one
   Congress.gov position ordinal from the same person observed again in a
   later full/incremental response.
*/
with nominee_responses as (
    {{ resolve_child('nomination_detail', 'nomination_nominee_detail', 'nominations') }}
), unnested as (
    select
        nominee_responses.raw_id as source_raw_id,
        nominee_responses.raw_response_version_id as source_raw_response_version_id,
        nominee_responses.latest_crawl_run_id as source_crawl_run_id,
        nominee_responses.parent_raw_id as nomination_detail_raw_id,
        nominee_responses.parent_endpoint_url as nomination_detail_endpoint_url,
        nominee_responses.child_fetched_at as source_fetched_at,
        nominee_responses.endpoint_url as source_endpoint_url,
        nominee_responses.page_offset as source_page_offset,
        nominee_responses.is_complete_generation as source_is_complete_generation,
        nominee_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        nominee_responses.generation_page_count as source_generation_page_count,
        nominee_responses.generation_expected_count as source_generation_expected_count,
        nominee_responses.generation_finished_at as source_generation_finished_at,
        substring(parent_endpoint_url from '/nomination/([0-9]+)/')::integer as congress,
        substring(parent_endpoint_url from '/nomination/[0-9]+/([0-9]+)') as nomination_number,
        lpad(coalesce(
            substring(parent_endpoint_url from '/nomination/[0-9]+/[0-9]+-([0-9]+)'),
            '00'
        ), 2, '0') as nomination_part,
        nominee.value as nominee_json,
        nominee.source_ordinal
    from nominee_responses
    cross join lateral jsonb_array_elements(
        coalesce(response #> '{nominees}', '[]'::jsonb)
    ) with ordinality as nominee(value, source_ordinal)
)

select
    source_raw_id,
    source_raw_response_version_id,
    source_crawl_run_id,
    nomination_detail_raw_id,
    nomination_detail_endpoint_url,
    source_fetched_at,
    source_endpoint_url,
    source_page_offset,
    source_is_complete_generation,
    source_generation_crawl_run_id,
    source_generation_page_count,
    source_generation_expected_count,
    source_generation_finished_at,
    source_ordinal,
    'pn' || nomination_number || '-' || congress::text as nomination_id,
    'pn' || nomination_number || '-' || nomination_part || '-' || congress::text as nomination_part_id,
    {{ json_int('nominee_json', 'ordinal') }} as position_id,
    {{ json_int('nominee_json', 'ordinal') }} as nominee_ordinal,
    {{ json_text('nominee_json', 'firstName') }} as first_name,
    {{ json_text('nominee_json', 'middleName') }} as middle_name,
    {{ json_text('nominee_json', 'lastName') }} as last_name,
    {{ json_text('nominee_json', 'prefix') }} as prefix,
    {{ json_text('nominee_json', 'suffix') }} as suffix,
    {{ json_text('nominee_json', 'state') }} as state,
    {{ json_date('nominee_json', 'effectiveDate') }} as effective_date,
    {{ json_text('nominee_json', 'predecessorName') }} as predecessor_name,
    {{ json_text('nominee_json', 'corpsCode') }} as corps_code
from unnested
