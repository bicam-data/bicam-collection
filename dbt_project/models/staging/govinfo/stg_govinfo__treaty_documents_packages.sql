{{ config(tags=['govinfo_treaty_documents', 'staging']) }}

with package_detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response as package_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'govinfo'
      and category = 'govinfo-treaty-documents'
      and node_type = 'govinfo_treaty_document_package_detail'
),
typed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        {{ json_text('package_json', 'packageId') }} as package_id,
        {{ json_int('package_json', 'congress') }} as congress_received,
        {{ json_int('package_json', 'documentNumber') }} as treaty_number,
        {{ json_int('package_json', 'session') }} as session,
        {{ json_int('package_json', 'pages') }} as pages,
        upper(coalesce(
            {{ json_text('package_json', 'docClass') }},
            {{ json_text('package_json', 'documentType') }}
        )) as document_class,
        {{ json_text('package_json', 'suDocClassNumber') }} as su_doc_class_number,
        coalesce(
            {{ json_text('package_json', 'otherIdentifier.migrated-doc-id') }},
            {{ json_text('package_json', 'migratedDocId') }}
        ) as migrated_doc_id,
        {{ json_text('package_json', 'collectionCode') }} as collection_code,
        {{ json_timestamptz('package_json', 'lastModified') }} as last_modified
    from package_detail
),
ranked as (
    select
        *,
        lower('td' || congress_received::text || '-' || treaty_number::text) as treaty_id,
        row_number() over (
            partition by congress_received, treaty_number
            order by last_modified desc nulls last, package_id
        ) as _rank
    from typed
    where document_class = 'TDOC'
      and congress_received is not null
      and treaty_number is not null
      and package_id ~* '^CDOC-[0-9]+tdoc[0-9]+$'
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    treaty_id,
    package_id,
    congress_received,
    treaty_number,
    document_class,
    su_doc_class_number,
    migrated_doc_id,
    collection_code,
    last_modified,
    session,
    pages
from ranked
where _rank = 1
