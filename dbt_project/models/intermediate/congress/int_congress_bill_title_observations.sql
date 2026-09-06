{{ config(materialized="table", tags=["bills", "congress", "child_state"]) }}

/*
   Resolver-selected bill-title occurrences with source lineage.

   Keep the selected child set materialized before joining parent payloads.
   The public bill-detail staging view parses every historical bill detail;
   joining it here made title propagation repeatedly scan and normalize the
   full detail archive. The selected child row already carries its exact
   parent raw ID, so derive the bill identity from only the parents needed by
   the selected title pages.
*/
with selected_title_responses as materialized (
    select
        raw_id,
        raw_response_version_id,
        latest_crawl_run_id,
        parent_id,
        parent_id as parent_raw_id,
        endpoint_url,
        page_offset,
        is_complete_generation,
        generation_crawl_run_id,
        generation_page_count,
        generation_expected_count,
        generation_finished_at,
        child_fetched_at,
        response
    from {{ ref('int_congress_child_response_selections') }}
    where category = 'bills'
      and node_type = 'bill_titles'
), bill_fields as materialized (
    select
        child.*,
        parent.endpoint_url as parent_endpoint_url,
        case
            when jsonb_typeof(parent.response -> 'bill') = 'array'
                then parent.response -> 'bill' -> 0
            else parent.response -> 'bill'
        end as bill_json
    from selected_title_responses as child
    join {{ ref('int_raw_responses__current') }} as parent
      on child.parent_id = parent.raw_id
    where parent.source = 'congress'
      and parent.category = 'bills'
      and parent.node_type = 'bill_detail'
), identity_rows as materialized (
    select
        bill_fields.*,
        lower(nullif(bill_json #>> '{type}', '')) as bill_type,
        nullif(bill_json #>> '{number}', '') as bill_number_text,
        coalesce(bill_json #>> '{congress}', '') as bill_congress
    from bill_fields
), title_responses as (
    /* Keep Roman numeral conversion separate so PostgreSQL does not plan a
       regexp-split window for every ordinary numeric bill title. */
    select
        identity_rows.*,
        bill_type || case
            when bill_number_text ~ '^[0-9]+$'
                then coalesce(nullif(ltrim(bill_number_text, '0'), ''), '0')
            when bill_number_text ~ '^[0-9]+½$'
                then regexp_replace(bill_number_text, '½$', '.5')
            when bill_number_text ~ '^[0-9]+¼$'
                then regexp_replace(bill_number_text, '¼$', '.25')
            when bill_number_text ~ '^[0-9]+¾$'
                then regexp_replace(bill_number_text, '¾$', '.75')
            else bill_number_text
        end || '-' || bill_congress as bill_id
    from identity_rows
    where bill_number_text is null
       or bill_number_text !~ '^[IVXLCDM]+$'
    union all
    select
        identity_rows.*,
        bill_type || {{ congress_bill_number('bill_json') }}
            || '-' || bill_congress as bill_id
    from identity_rows
    where bill_number_text ~ '^[IVXLCDM]+$'
), unnested as (
    select
        title_responses.raw_id as source_raw_id,
        title_responses.raw_response_version_id as source_raw_response_version_id,
        title_responses.latest_crawl_run_id as source_crawl_run_id,
        title_responses.parent_raw_id as bill_detail_raw_id,
        title_responses.parent_endpoint_url as bill_detail_endpoint_url,
        title_responses.endpoint_url as source_endpoint_url,
        title_responses.child_fetched_at as source_fetched_at,
        title_responses.page_offset as source_page_offset,
        title_responses.is_complete_generation as source_is_complete_generation,
        title_responses.generation_crawl_run_id as source_generation_crawl_run_id,
        title_responses.generation_page_count as source_generation_page_count,
        title_responses.generation_expected_count as source_generation_expected_count,
        title_responses.generation_finished_at as source_generation_finished_at,
        titles.value as title_json,
        titles.array_ordinal as source_ordinal,
        title_responses.bill_id
    from title_responses
    cross join lateral jsonb_array_elements(
        coalesce(title_responses.response -> 'titles', '[]'::jsonb)
    ) with ordinality as titles(value, array_ordinal)
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
    nullif(title_json #>> '{title}', '') as title,
    nullif(title_json #>> '{titleType}', '') as title_type,
    lower(nullif(title_json #>> '{chamberName}', '')) as chamber,
    nullif(title_json #>> '{titleTypeCode}', '') as title_type_code,
    nullif(title_json #>> '{billTextVersionName}', '') as bill_text_version_name,
    nullif(title_json #>> '{billTextVersionCode}', '') as bill_text_version_code,
    (nullif(title_json #>> '{updateDate}', ''))::timestamptz as updated_at
from unnested
