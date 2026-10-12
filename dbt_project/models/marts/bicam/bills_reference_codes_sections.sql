{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_code_id', 'code_section'], 'unique': true}]) }}
with packages as (
    -- Preserve every harvested package so section rows remain aligned with
    -- the package-grain parent reference table.
    select p.package_id, p.raw_id
    from {{ ref('stg_govinfo__bills_packages') }} p
    join {{ ref('bills') }} b
      on b.bill_id = p.bill_type || p.bill_number::text || '-' || p.congress::text
), contents as (
    select p.package_id, ref.ordinality as ref_n, content.ordinality as content_n,
        content.value as content_json
    from packages p
    join {{ ref('int_raw_responses__current') }} raw on raw.raw_id = p.raw_id
    cross join lateral jsonb_array_elements(coalesce(raw.response->'references', '[]'::jsonb)) with ordinality ref(value, ordinality)
    cross join lateral jsonb_array_elements(coalesce(ref.value->'contents', '[]'::jsonb)) with ordinality content(value, ordinality)
    where upper(coalesce(ref.value->>'collectionCode', '')) = 'USCODE'
)
select {{ hash_to_uuid("package_id || '|usc|' || ref_n::text || '|' || content_n::text") }}::text as bill_code_id,
    section.value as code_section
from contents
cross join lateral jsonb_array_elements_text(coalesce(content_json->'sections', '[]'::jsonb)) section(value)
