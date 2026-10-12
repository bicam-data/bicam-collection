{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id', 'law_id']}]) }}
with packages as (
    -- GovInfo references are package observations; bills_metadata is only the
    -- representative package for bill-level metadata.
    select p.package_id, p.raw_id,
      p.bill_type || p.bill_number::text || '-' || p.congress::text as bill_id
    from {{ ref('stg_govinfo__bills_packages') }} p
    join {{ ref('bills') }} b
      on b.bill_id = p.bill_type || p.bill_number::text || '-' || p.congress::text
), refs as (
    select p.bill_id,
      ref.value as ref_json
    from packages p
    join {{ ref('int_raw_responses__current') }} raw on raw.raw_id = p.raw_id
    cross join lateral jsonb_array_elements(coalesce(raw.response->'references', '[]'::jsonb)) ref(value)
    where upper(coalesce(ref.value->>'collectionCode', '')) = 'PLAW'
), contents as (
    select bill_id, content.value as law_json
    from refs cross join lateral jsonb_array_elements(coalesce(ref_json->'contents', '[]'::jsonb)) content(value)
)
select distinct bill_id,
    'PL' || nullif(trim(law_json->>'congress'), '') || '-' || nullif(trim(law_json->>'number'), '') as law_id,
    case lower(trim(coalesce(law_json->>'label', '')))
      when 'public law' then 'Public Law'
      when 'private law' then 'Private Law'
      else nullif(trim(law_json->>'label'), '')
    end as law_type
from contents
