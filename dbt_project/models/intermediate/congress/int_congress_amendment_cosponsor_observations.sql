{{ config(materialized="table", tags=["amendments", "congress", "reconciliation"]) }}

/*
   Preserve every selected amendment-cosponsor array occurrence together with
   the response-version and generation evidence that selected it.  The public
   relationship grain is amendment/member; this relation is the audit grain
   used by reciprocity and by future natural-key reconciliation.  In
   particular, it must not read only ``int_raw_responses__current`` because
   adjacent non-overlapping incremental windows are legitimate observations.
*/
with cosponsor_responses as (
    {{ resolve_child('amendment_detail', 'amendment_cosponsors', 'amendments') }}
), amendment_detail as (
    select raw_id, amendment_id
    from {{ ref('stg_congress__amendment_detail') }}
), unnested as (
    select
        cosponsor_responses.raw_id as source_raw_id,
        cosponsor_responses.raw_response_version_id as source_raw_response_version_id,
        cosponsor_responses.latest_crawl_run_id as source_crawl_run_id,
        cosponsor_responses.child_fetched_at as source_fetched_at,
        cosponsor_responses.endpoint_url as source_endpoint_url,
        cosponsor_responses.page_offset as source_page_offset,
        cosponsor_responses.is_complete_generation as source_is_complete_generation,
        cosponsor_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        cosponsor_responses.generation_page_count as source_generation_page_count,
        cosponsor_responses.generation_expected_count as source_generation_expected_count,
        cosponsor_responses.generation_finished_at as source_generation_finished_at,
        amendment_detail.amendment_id,
        cosponsor.value as cosponsor_json,
        cosponsor.source_ordinal
    from cosponsor_responses
    join amendment_detail
      on cosponsor_responses.parent_raw_id = amendment_detail.raw_id
    cross join lateral jsonb_array_elements(
        coalesce(cosponsor_responses.response -> 'cosponsors', '[]'::jsonb)
    ) with ordinality as cosponsor(value, source_ordinal)
)
select
    source_raw_id,
    source_raw_response_version_id,
    source_crawl_run_id,
    source_fetched_at,
    source_endpoint_url,
    source_page_offset,
    source_is_complete_generation,
    source_generation_crawl_run_id,
    source_generation_page_count,
    source_generation_expected_count,
    source_generation_finished_at,
    source_ordinal,
    amendment_id,
    {{ json_text('cosponsor_json', 'bioguideId') }} as bioguide_id,
    {{ json_text('cosponsor_json', 'fullName') }} as display_name,
    {{ json_text('cosponsor_json', 'firstName') }} as first_name,
    {{ json_text('cosponsor_json', 'middleName') }} as middle_name,
    {{ json_text('cosponsor_json', 'lastName') }} as last_name,
    {{ json_text('cosponsor_json', 'party') }} as party,
    {{ json_text('cosponsor_json', 'state') }} as state,
    {{ json_int('cosponsor_json', 'district') }} as district,
    {{ json_boolean('cosponsor_json', 'isOriginalCosponsor') }} as is_original_cosponsor,
    {{ json_date('cosponsor_json', 'sponsorshipDate') }} as sponsorship_date,
    {{ json_date('cosponsor_json', 'sponsorshipWithdrawnDate') }} as sponsorship_withdrawal_date
from unnested
