{{ config(
    materialized="table",
    indexes=[
        {"columns": ["format_key"], "unique": true},
        {"columns": ["print_id"]}
    ],
    tags=["committee_prints", "raw", "child_state"]
) }}

/* Committee-print text is a paginated child collection.  Keep one stable
   observation row per text/format array position after the child resolver has
   selected a complete baseline plus applicable deltas.  The public mart can
   then choose one current rendition per print/type without losing the source
   evidence needed to explain that choice. */
with text_responses as (
    {{ resolve_child('committee_print_detail', 'committee_print_texts', 'committee-prints') }}
),
text_items as (
    select
        text_responses.raw_id as text_raw_id,
        text_responses.endpoint_url as text_endpoint_url,
        text_responses.raw_response_version_id,
        text_responses.parent_raw_id as print_detail_raw_id,
        text_responses.child_fetched_at,
        text_responses.is_complete_generation as source_is_complete_generation,
        lower(left((regexp_match(text_responses.endpoint_url, '/committee-print/[0-9]+/([^/]+)/'))[1], 1))
          || 'prt'
          || ((regexp_match(text_responses.endpoint_url, '/committee-print/[0-9]+/[^/]+/([0-9]+)/'))[1])
          || '-'
          || ((regexp_match(text_responses.endpoint_url, '/committee-print/([0-9]+)/'))[1]) as print_id,
        text_item.ordinality::integer as text_item_ordinal,
        text_item.value as text_item_json
    from text_responses
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(text_responses.response -> 'text') = 'array'
                then text_responses.response -> 'text'
            else '[]'::jsonb
        end
    ) with ordinality as text_item(value, ordinality)
), formats as (
    select
        text_items.*,
        format_item.ordinality::integer as format_ordinal,
        format_item.value as format_json
    from text_items
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(text_items.text_item_json -> 'formats') = 'array'
                then text_items.text_item_json -> 'formats'
            /* The live Congress.gov committee-print endpoint also publishes
               the rendition object directly in `text` (without a nested
               `formats` array).  Treat that shape as one format while
               retaining the nested shape used by older payloads/fixtures. */
            when jsonb_typeof(text_items.text_item_json) = 'object'
                 and text_items.text_item_json ? 'url'
                then jsonb_build_array(text_items.text_item_json)
            else '[]'::jsonb
        end
    ) with ordinality as format_item(value, ordinality)
), keyed as (
    select
        formats.*,
        text_raw_id::text || ':' || text_item_ordinal::text || ':' || format_ordinal::text as format_key
    from formats
)

select distinct on (format_key)
    format_key,
    text_raw_id,
    text_endpoint_url,
    print_detail_raw_id,
    raw_response_version_id,
    child_fetched_at,
    source_is_complete_generation,
    print_id,
    text_item_ordinal,
    format_ordinal,
    format_json
from keyed
order by format_key, child_fetched_at desc nulls last, raw_response_version_id desc nulls last
