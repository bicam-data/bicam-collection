{{ config(tags=['govinfo_congressional_directories', 'staging']) }}

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    {{ json_text('response', 'packageId') }} as package_id,
    {{ json_int('response', 'congress') }} as congress_number,
    {{ json_text('response', 'title') }} as title,
    {{ json_date('response', 'dateIssued') }} as issued_at,
    {{ json_text('response', 'suDocClassNumber') }} as su_doc_class_number,
    {{ json_text('response', 'otherIdentifier.migrated-doc-id') }} as migrated_doc_id,
    {{ json_text('response', 'otherIdentifier.ils-system-id') }} as ils_system_id,
    {{ json_text('response', 'collectionCode') }} as collection_code,
    {{ json_text('response', 'governmentAuthor1') }} as government_author1,
    {{ json_text('response', 'governmentAuthor2') }} as government_author2,
    {{ json_text('response', 'publisher') }} as publisher,
    {{ json_timestamptz('response', 'lastModified') }} as last_modified
from {{ ref('int_raw_responses__current') }}
where source = 'govinfo'
  and category = 'govinfo-congressional-directories'
  and node_type = 'govinfo_congressional_directory_package_detail'

