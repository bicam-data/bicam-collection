{{ config(
    materialized="table",
    indexes=[{"columns": ["format_key"], "unique": true}, {"columns": ["bill_id"]}]
) }}

select
    versions.text_version_key,
    versions.text_raw_id,
    versions.raw_response_version_id,
    versions.bill_id,
    versions.text_version_ordinal,
    format_item.ordinality::integer as format_ordinal,
    versions.text_version_key || ':' || format_item.ordinality::text as format_key,
    versions.text_version_json,
    format_item.value as format_json
from {{ ref('int_congress_bill_text_versions') }} as versions
cross join lateral jsonb_array_elements(
    case
        when jsonb_typeof(versions.text_version_json -> 'formats') = 'array'
            then versions.text_version_json -> 'formats'
        else '[]'::jsonb
    end
) with ordinality as format_item(value, ordinality)
