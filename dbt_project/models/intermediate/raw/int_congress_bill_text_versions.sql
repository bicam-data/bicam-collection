{{ config(
    materialized="table",
    indexes=[{"columns": ["text_version_key"], "unique": true}, {"columns": ["bill_id"]}]
) }}

/* The bill text child is a paginated/delta collection. Resolve its complete
   baseline and applicable non-empty deltas before expanding textVersions;
   selecting only the current URL would lose versions returned in an earlier
   non-overlapping window.

   Keep the filtered child set materialized before joining parent payloads.
   The public bill-detail staging view parses every historical bill detail;
   using it here made this one text expansion repeatedly scan and normalize
   400k+ rows. The child selection already carries the exact parent raw ID, so
   this bounded join preserves the same lineage while limiting bill-ID JSON
   extraction to parents that actually have selected text pages. */
with selected_text_responses as materialized (
    select
        raw_id,
        raw_response_version_id,
        parent_id,
        endpoint_url,
        is_complete_generation,
        generation_crawl_run_id,
        generation_page_count,
        generation_expected_count,
        generation_finished_at,
        child_fetched_at,
        response
    from {{ ref('int_congress_child_response_selections') }}
    where category = 'bills'
      and node_type = 'bill_text'
), bill_fields as materialized (
    select
        child.*,
        case
            when jsonb_typeof(parent.response -> 'bill') = 'array'
                then parent.response -> 'bill' -> 0
            else parent.response -> 'bill'
        end as bill_json
    from selected_text_responses as child
    join {{ ref('int_raw_responses__current') }} as parent
      on parent.raw_id = child.parent_id
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
), text_responses as (
    /* Numeric/fraction bill numbers are the normal case. Keep the Roman
       numeral conversion in a separate branch so PostgreSQL does not plan a
       regexp-split window for every one of the 400k bill details. */
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
)
select
    text_response.raw_id as text_raw_id,
    text_response.parent_id as bill_detail_raw_id,
    text_response.endpoint_url as text_endpoint_url,
    text_response.raw_response_version_id,
    text_response.child_fetched_at as source_fetched_at,
    text_response.is_complete_generation as source_is_complete_generation,
    text_response.generation_crawl_run_id as source_generation_crawl_run_id,
    text_response.generation_page_count as source_generation_page_count,
    text_response.generation_expected_count as source_generation_expected_count,
    text_response.generation_finished_at as source_generation_finished_at,
    text_response.bill_id,
    text_version.ordinality::integer as text_version_ordinal,
    text_response.raw_id::text || ':' || text_version.ordinality::text as text_version_key,
    text_version.value as text_version_json
from text_responses as text_response
cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(text_response.response -> 'textVersions') = 'array'
            then text_response.response -> 'textVersions'
        else '[]'::jsonb
    end
) with ordinality as text_version(value, ordinality)
