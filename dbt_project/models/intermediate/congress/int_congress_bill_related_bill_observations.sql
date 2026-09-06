{{ config(
    materialized="table",
    tags=["bills", "congress", "child_state"]
) }}

/*
   Preserve every resolver-selected related-bill occurrence before the public
   bill-to-bill edge is reduced to its natural key.  The public staging model
   intentionally keeps only the existing columns, while this relation keeps
   the raw-version, crawl, page, generation, and array-position evidence that
   explains why a current edge was selected.
*/
with related_bill_responses as (
    {{ resolve_child('bill_detail', 'bill_relatedbills', 'bills') }}
), parent_bills as (
    select
        related_bill_responses.raw_id as source_raw_id,
        related_bill_responses.raw_response_version_id as source_raw_response_version_id,
        related_bill_responses.latest_crawl_run_id as source_crawl_run_id,
        related_bill_responses.parent_raw_id as bill_detail_raw_id,
        related_bill_responses.parent_endpoint_url as bill_detail_endpoint_url,
        related_bill_responses.endpoint_url as source_endpoint_url,
        related_bill_responses.child_fetched_at as source_fetched_at,
        related_bill_responses.page_offset as source_page_offset,
        related_bill_responses.is_complete_generation as source_is_complete_generation,
        related_bill_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        related_bill_responses.generation_page_count as source_generation_page_count,
        related_bill_responses.generation_expected_count as source_generation_expected_count,
        related_bill_responses.generation_finished_at as source_generation_finished_at,
        related_bill_responses.response,
        regexp_match(
            related_bill_responses.parent_endpoint_url,
            '/bill/([0-9]+)/([^/]+)/([^/?]+)'
        ) as bill_components
    from related_bill_responses
), related_bills as (
    select
        parent_bills.*,
        lower(parent_bills.bill_components[2])
        || {{ normalize_congress_bill_number_text('parent_bills.bill_components[3]') }}
        || '-' || parent_bills.bill_components[1] as bill_id,
        related_bill.value as related_bill_json,
        related_bill.ordinality as source_ordinal
    from parent_bills
    cross join lateral jsonb_array_elements(
        coalesce(parent_bills.response -> 'relatedBills', '[]'::jsonb)
    ) with ordinality as related_bill(value, ordinality)
    where parent_bills.bill_components is not null
), relationships as (
    select
        related_bills.*,
        relationship.value as relationship_json,
        relationship.ordinality as relationship_ordinal
    from related_bills
    cross join lateral jsonb_array_elements(
        coalesce(related_bills.related_bill_json -> 'relationshipDetails', '[]'::jsonb)
    ) with ordinality as relationship(value, ordinality)
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
    relationship_ordinal,
    bill_id,
    lower(nullif(related_bill_json #>> '{type}', ''))
        || {{ normalize_congress_bill_number_text("nullif(related_bill_json #>> '{number}', '')") }}
        || '-' || coalesce(related_bill_json #>> '{congress}', '') as relatedbill_id,
    lower(nullif(related_bill_json #>> '{type}', '')) as relatedbill_type,
    {{ normalize_congress_bill_number_text("nullif(related_bill_json #>> '{number}', '')") }} as relatedbill_number,
    (nullif(related_bill_json #>> '{congress}', ''))::integer as congress,
    nullif(relationship_json #>> '{type}', '') as relationship_type,
    nullif(relationship_json #>> '{identifiedBy}', '') as relationship_identified_by,
    source_raw_id::text || ':' || source_ordinal::text || ':' || relationship_ordinal::text
        as observation_key
from relationships
