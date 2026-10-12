{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_statute_id', 'page'], 'unique': true}]) }}
with packages as (
    -- Page rows use the same all-package parent identity as statutes.
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
    where upper(coalesce(ref.value->>'collectionCode', '')) = 'STATUTE'
)
select {{ hash_to_uuid("package_id || '|statute|' || ref_n::text || '|' || content_n::text") }}::text as bill_statute_id,
    page.value as page
from contents
cross join lateral jsonb_array_elements_text(coalesce(content_json->'pages', '[]'::jsonb)) page(value)
