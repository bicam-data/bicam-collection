{{ config(tags=["bills", "congress"]) }}

/*
   Preserve the source occurrence needed to reconcile repeated full/delta
   child responses.  The public/staging relation has a bill/member grain, but
   this intermediate keeps every array occurrence so that a repeated fetch is
   distinguishable from a source payload that genuinely contains two rows.
*/
with cosponsor_responses as (
    {{ resolve_child('bill_detail', 'bill_cosponsors', 'bills') }}
), bill_detail as (
    select
        raw_id,
        bill_id
    from {{ ref('stg_congress__bill_detail') }}
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
        bill_detail.bill_id,
        cosponsor.value as cosponsor_json,
        cosponsor.source_ordinal
    from cosponsor_responses
    join bill_detail
      on cosponsor_responses.parent_raw_id = bill_detail.raw_id
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
    bill_id,
    {{ json_text('cosponsor_json', 'bioguideId') }} as bioguide_id,
    {{ json_text('cosponsor_json', 'fullName') }} as display_name,
    {{ json_text('cosponsor_json', 'party') }} as party,
    {{ json_text('cosponsor_json', 'state') }} as state,
    {{ json_int('cosponsor_json', 'district') }} as district,
    coalesce({{ json_boolean('cosponsor_json', 'isOriginalCosponsor') }}, false) as is_original_cosponsor,
    {{ json_date('cosponsor_json', 'sponsorshipDate') }} as sponsorship_date,
    {{ json_date('cosponsor_json', 'sponsorshipWithdrawnDate') }} as sponsorship_withdrawal_date
from unnested
