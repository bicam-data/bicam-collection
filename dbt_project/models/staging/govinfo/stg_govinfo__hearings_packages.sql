{{ config(tags=["govinfo_hearings", "staging", "evidence"]) }}

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    {{ json_text('response', 'packageId') }} as package_id,
    {{ json_text('response', 'title') }} as title,
    {{ json_int('response', 'congress') }} as congress,
    {{ json_text('response', 'chamber') }} as chamber,
    {{ json_date('response', 'dateIssued') }} as issued_at,
    {{ json_timestamptz('response', 'lastModified') }} as last_modified,
    {{ json_text('response', 'collectionCode') }} as collection_code
    from {{ ref('int_raw_responses__current') }}
where source = 'govinfo'
  and category = 'govinfo-hearings'
  and node_type = 'govinfo_hearing_package_detail'
