{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_statute_id'], 'unique': true}, {'columns': ['bill_id']}]) }}
with packages as (
    -- Keep all package versions, matching the published package-grain child
    -- table rather than the latest-only bill metadata projection.
    select p.package_id, p.raw_id,
      p.bill_type || p.bill_number::text || '-' || p.congress::text as bill_id
    from {{ ref('stg_govinfo__bills_packages') }} p
    join {{ ref('bills') }} b
      on b.bill_id = p.bill_type || p.bill_number::text || '-' || p.congress::text
), refs as (
    select p.bill_id,
      p.package_id, ref.value as ref_json, ref.ordinality as ref_n
    from packages p
    join {{ ref('int_raw_responses__current') }} raw on raw.raw_id = p.raw_id
    cross join lateral jsonb_array_elements(coalesce(raw.response->'references', '[]'::jsonb)) with ordinality ref(value, ordinality)
    where upper(coalesce(ref.value->>'collectionCode', '')) = 'STATUTE'
), contents as (
    select bill_id, package_id, ref_n,
        content.value->>'label' as reference_label,
        content.value as content_json, content.ordinality as content_n
    from refs cross join lateral jsonb_array_elements(coalesce(ref_json->'contents', '[]'::jsonb)) with ordinality content(value, ordinality)
)
select {{ hash_to_uuid("package_id || '|statute|' || ref_n::text || '|' || content_n::text") }}::text as bill_statute_id,
    bill_id,
    concat_ws('',
        nullif(trim(lower(reference_label)), ''),
        nullif(trim(content_json->>'title'), '')
    ) as reference_statute
from contents
