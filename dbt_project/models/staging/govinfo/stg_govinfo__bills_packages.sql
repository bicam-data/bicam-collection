{{ config(tags=["govinfo_bills", "staging"]) }}

with package_detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response as bill_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'govinfo'
      and category = 'govinfo-bills'
      and node_type = 'govinfo_bill_package_detail'
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    {{ json_text('bill_json', 'packageId') }} as package_id,
    lower({{ json_text('bill_json', 'billType') }}) as bill_type,
    {{ json_int('bill_json', 'billNumber') }} as bill_number,
    {{ json_int('bill_json', 'congress') }} as congress,
    -- A small number of legacy package summaries expose only the extended
    -- version field (for example BILLS-108hr10v1rh).  Preserve that source
    -- value instead of turning an otherwise valid bill package into a null
    -- version row.
    coalesce(
        {{ json_text('bill_json', 'billVersion') }},
        {{ json_text('bill_json', 'billVersionExtended') }}
    ) as bill_version,
    {{ json_text('bill_json', 'title') }} as title,
    {{ json_text('bill_json', 'shortTitle') }} as short_title,
    {{ json_text('bill_json', 'originChamber') }} as origin_chamber,
    {{ json_text('bill_json', 'currentChamber') }} as current_chamber,
    {{ json_date('bill_json', 'dateIssued') }} as issued_at,
    {{ json_timestamptz('bill_json', 'lastModified') }} as last_modified,
    {{ json_text('bill_json', 'collectionCode') }} as collection_code
from package_detail
