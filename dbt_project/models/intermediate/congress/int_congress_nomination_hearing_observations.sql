{{ config(materialized="table", tags=["nominations", "congress", "child_state"]) }}

/* Resolver-selected nomination hearing observations with source lineage. */
with hearing_responses as (
    {{ resolve_child('nomination_detail', 'nomination_hearings', 'nominations') }}
), unnested as (
    select
        hearing_responses.raw_id as source_raw_id,
        hearing_responses.raw_response_version_id as source_raw_response_version_id,
        hearing_responses.latest_crawl_run_id as source_crawl_run_id,
        hearing_responses.parent_raw_id as nomination_detail_raw_id,
        hearing_responses.parent_endpoint_url as nomination_detail_endpoint_url,
        hearing_responses.endpoint_url as source_endpoint_url,
        hearing_responses.child_fetched_at as source_fetched_at,
        hearing_responses.page_offset as source_page_offset,
        hearing_responses.is_complete_generation as source_is_complete_generation,
        hearing_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        hearing_responses.generation_page_count as source_generation_page_count,
        hearing_responses.generation_expected_count as source_generation_expected_count,
        hearing_responses.generation_finished_at as source_generation_finished_at,
        substring(hearing_responses.parent_endpoint_url from '/nomination/([0-9]+)/')::integer as nomination_congress,
        substring(hearing_responses.parent_endpoint_url from '/nomination/[0-9]+/([0-9]+)') as nomination_number,
        lpad(coalesce(
            substring(hearing_responses.parent_endpoint_url from '/nomination/[0-9]+/[0-9]+-([0-9]+)'),
            '00'
        ), 2, '0') as nomination_part,
        hearing.value as hearing_json,
        hearing.array_ordinal as source_ordinal
    from hearing_responses
    cross join lateral jsonb_array_elements(
        coalesce(hearing_responses.response -> 'hearings', '[]'::jsonb)
    ) with ordinality as hearing(value, array_ordinal)
), parsed as (
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
        'pn' || nomination_number || '-' || nomination_congress::text as nomination_id,
        'pn' || nomination_number || '-' || nomination_part || '-' || nomination_congress::text as nomination_part_id,
        {{ congress_hearing_route_chamber(json_text('hearing_json', 'chamber')) }} as chamber,
        coalesce(
            nullif(substring({{ json_text('hearing_json', 'url') }} from '/hearing/([0-9]+)/'), ''),
            nomination_congress::text
        )::integer as congress,
        coalesce({{ json_text('hearing_json', 'partNumber') }}, '0') as hearing_part,
        coalesce({{ json_bigint('hearing_json', 'jacketNumber') }}::text, 'MISSING') as hearing_jacketnumber,
        hearing_json
    from unnested
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
    nomination_id,
    nomination_part_id,
    lower(left(chamber, 1)) || 'hrg' || hearing_jacketnumber || '-' || congress::text as hearing_id,
    lower(left(chamber, 1)) || 'hrg' || hearing_jacketnumber || '-' || hearing_part || '-' || congress::text as hearing_part_id,
    chamber,
    hearing_jacketnumber,
    congress,
    {{ json_text('hearing_json', 'number') }} as hearing_number,
    hearing_part,
    {{ json_date('hearing_json', 'date') }} as hearing_date,
    {{ json_text('hearing_json', 'errataNumber') }} as "errataNumber",
    {{ json_text('hearing_json', 'citation') }} as citation
from parsed
