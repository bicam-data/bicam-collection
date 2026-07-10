{{ config(tags=["govinfo_bills", "staging"]) }}

with package_detail as (
    select
        response as bill_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'govinfo'
      and category = 'govinfo-bills'
      and node_type = 'govinfo_bill_package_detail'
)

select
    {{ json_text('bill_json', 'packageId') }} as package_id,
    lower({{ json_text('bill_json', 'billType') }}) as bill_type,
    {{ json_int('bill_json', 'billNumber') }} as bill_number,
    {{ json_int('bill_json', 'congress') }} as congress,
    {{ json_text('bill_json', 'billVersion') }} as bill_version,
    {{ json_text('bill_json', 'title') }} as title,
    {{ json_text('bill_json', 'shortTitle') }} as short_title,
    {{ json_text('bill_json', 'originChamber') }} as origin_chamber,
    {{ json_text('bill_json', 'currentChamber') }} as current_chamber,
    {{ json_date('bill_json', 'dateIssued') }} as issued_at,
    {{ json_timestamptz('bill_json', 'lastModified') }} as last_modified,
    {{ json_text('bill_json', 'collectionCode') }} as collection_code
from package_detail
