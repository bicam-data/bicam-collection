{{ config(materialized="table", tags=["committees", "bills", "congress", "child_state"]) }}

/* Resolver-selected committee-to-bill observations with source lineage. */
with bill_responses as (
    {{ resolve_child('committee_detail', 'committee_bills', 'committees') }}
), unnested as (
    select
        bill_responses.raw_id as source_raw_id,
        bill_responses.raw_response_version_id as source_raw_response_version_id,
        bill_responses.latest_crawl_run_id as source_crawl_run_id,
        bill_responses.parent_raw_id as committee_detail_raw_id,
        bill_responses.parent_endpoint_url as committee_detail_endpoint_url,
        bill_responses.endpoint_url as source_endpoint_url,
        bill_responses.child_fetched_at as source_fetched_at,
        bill_responses.page_offset as source_page_offset,
        bill_responses.is_complete_generation as source_is_complete_generation,
        bill_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        bill_responses.generation_page_count as source_generation_page_count,
        bill_responses.generation_expected_count as source_generation_expected_count,
        bill_responses.generation_finished_at as source_generation_finished_at,
        bills.value as bill_json,
        bills.array_ordinal as source_ordinal,
        committees.committee_id
    from bill_responses
    join {{ ref('stg_congress__committee_detail') }} as committees
      on bill_responses.parent_raw_id = committees.raw_id
    cross join lateral jsonb_array_elements(
        coalesce(bill_responses.response #> '{committee-bills,bills}', '[]'::jsonb)
    ) with ordinality as bills(value, array_ordinal)
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
        lower({{ json_text('bill_json', 'type') }})
        || {{ normalize_congress_bill_number_text(json_text('bill_json', 'number')) }}
        || '-' || {{ json_int('bill_json', 'congress') }}::text as bill_id,
        lower({{ json_text('bill_json', 'type') }}) as bill_type,
        {{ normalize_congress_bill_number_text(json_text('bill_json', 'number')) }} as bill_number,
        {{ json_int('bill_json', 'congress') }} as congress,
        {{ json_text('bill_json', 'relationshipType') }} as relationship_type,
        {{ json_timestamptz('bill_json', 'actionDate') }} as committee_action_date,
        {{ json_timestamptz('bill_json', 'updateDate') }} as updated_at
    from unnested
)
select *
from parsed
