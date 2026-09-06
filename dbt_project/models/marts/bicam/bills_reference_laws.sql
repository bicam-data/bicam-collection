{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id', 'law_id']}]) }}
with refs as (
    select p.bill_type || p.bill_number::text || '-' || p.congress::text as bill_id,
      ref.value as ref_json
    from {{ ref('stg_govinfo__bills_packages') }} p
    join {{ ref('bills_metadata') }} m on m.govinfo_package_id = p.package_id
    join {{ ref('int_raw_responses__current') }} raw on raw.raw_id = p.raw_id
    cross join lateral jsonb_array_elements(coalesce(raw.response->'references', '[]'::jsonb)) ref(value)
    where upper(coalesce(ref.value->>'collectionCode', '')) = 'PLAW'
), contents as (
    select bill_id, content.value as law_json
    from refs cross join lateral jsonb_array_elements(coalesce(ref_json->'contents', '[]'::jsonb)) content(value)
)
select distinct bill_id,
    'pl' || nullif(law_json->>'congress', '') || '-' || nullif(law_json->>'number', '') as law_id,
    lower(split_part(coalesce(law_json->>'label', ''), ' ', 1)) as law_type
from contents
