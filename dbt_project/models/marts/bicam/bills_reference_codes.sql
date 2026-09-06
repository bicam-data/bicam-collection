{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_code_id'], 'unique': true}, {'columns': ['bill_id']}]) }}
with bill_refs as (
    select p.bill_type || p.bill_number::text || '-' || p.congress::text as bill_id,
        p.package_id, ref.value as reference_json, ref.ordinality as ref_n
    from {{ ref('stg_govinfo__bills_packages') }} p
    join {{ ref('bills_metadata') }} m on m.govinfo_package_id = p.package_id
    join {{ ref('int_raw_responses__current') }} raw on raw.raw_id = p.raw_id
    cross join lateral jsonb_array_elements(coalesce(raw.response->'references', '[]'::jsonb)) with ordinality ref(value, ordinality)
    where upper(coalesce(ref.value->>'collectionCode', '')) = 'USCODE'
), contents as (
    select bill_id, package_id, ref_n, content.value as content_json, content.ordinality as content_n
    from bill_refs cross join lateral jsonb_array_elements(coalesce(reference_json->'contents', '[]'::jsonb)) with ordinality content(value, ordinality)
)
select {{ hash_to_uuid("package_id || '|usc|' || ref_n::text || '|' || content_n::text") }}::text as bill_code_id,
    bill_id, 'usc' || nullif(content_json->>'title', '') as reference_code
from contents
