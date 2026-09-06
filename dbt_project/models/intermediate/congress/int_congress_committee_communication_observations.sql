{{ config(materialized="table", tags=["committees", "congress", "child_state"]) }}

/* Resolver-selected committee communication observations with source lineage. */
with communication_responses as (
    {{ resolve_child('committee_detail', 'committee_communications', 'committees') }}
), unnested as (
    select
        communication_responses.raw_id as source_raw_id,
        communication_responses.raw_response_version_id as source_raw_response_version_id,
        communication_responses.latest_crawl_run_id as source_crawl_run_id,
        communication_responses.parent_raw_id as committee_detail_raw_id,
        communication_responses.parent_endpoint_url as committee_detail_endpoint_url,
        communication_responses.endpoint_url as source_endpoint_url,
        communication_responses.child_fetched_at as source_fetched_at,
        communication_responses.page_offset as source_page_offset,
        communication_responses.is_complete_generation as source_is_complete_generation,
        communication_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        communication_responses.generation_page_count as source_generation_page_count,
        communication_responses.generation_expected_count as source_generation_expected_count,
        communication_responses.generation_finished_at as source_generation_finished_at,
        communication.value as communication_json,
        communication.array_ordinal as source_ordinal,
        committees.committee_id
    from communication_responses
    join {{ ref('stg_congress__committee_detail') }} as committees
      on communication_responses.parent_raw_id = committees.raw_id
    cross join lateral jsonb_array_elements(
        coalesce(communication_responses.response -> 'houseCommunications', '[]'::jsonb)
        || coalesce(communication_responses.response -> 'senateCommunications', '[]'::jsonb)
    ) with ordinality as communication(value, array_ordinal)
)
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
    lower({{ json_text('communication_json', 'chamber') }})
        || '-'
        || lower({{ json_text('communication_json', 'communicationType.code') }})
        || {{ json_int('communication_json', 'number') }}::text
        || '-'
        || {{ json_int('communication_json', 'congress') }}::text as communication_id,
    lower({{ json_text('communication_json', 'chamber') }}) as chamber,
    lower({{ json_text('communication_json', 'communicationType.code') }}) as communication_type_code,
    {{ json_text('communication_json', 'communicationType.name') }} as communication_type_name,
    {{ json_int('communication_json', 'congress') }} as congress,
    {{ json_int('communication_json', 'number') }} as communication_number,
    {{ json_date('communication_json', 'referralDate') }} as referral_date,
    {{ json_timestamptz('communication_json', 'updateDate') }} as updated_at,
    {{ json_text('communication_json', 'url') }} as communication_url
from unnested
