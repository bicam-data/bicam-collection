{{ config(tags=["govinfo_committee_reports", "staging"]) }}

with package_detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response as package_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'govinfo'
      and category = 'govinfo-committee-reports'
      and node_type = 'govinfo_committee_report_package_detail'
),
normalized as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        package_json,
        {{ json_text('package_json', 'packageId') }} as package_id,
        lower(coalesce(
            {{ json_text('package_json', 'docClass') }},
            {{ json_text('package_json', 'documentType') }}
        )) as report_type,
        {{ json_text('package_json', 'documentNumber') }} as document_number_text,
        {{ json_int('package_json', 'congress') }} as congress,
        {{ json_int('package_json', 'session') }} as session
    from package_detail
),
typed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        package_id,
        {{ govinfo_committeereport_report_set_id_from_fields(
            'report_type',
            'document_number_text',
            'congress',
            'null'
        ) }} as report_set_id,
        report_type,
        document_number_text::integer as report_number,
        coalesce({{ json_int('package_json', 'documentPart') }}, 1) as report_part,
        {{ json_text('package_json', 'title') }} as title,
        {{ json_text('package_json', 'subtitle') }} as subtitle,
        lower({{ json_text('package_json', 'chamber') }}) as chamber,
        congress,
        session,
        {{ json_int('package_json', 'pages') }} as pages,
        {{ govinfo_committeereport_is_errata(
            json_text('package_json', 'title'),
            'null',
            'null'
        ) }} as is_errata,
        {{ json_date('package_json', 'dateIssued') }} as issued_at,
        lower({{ json_text('package_json', 'branch') }}) as branch,
        {{ json_text('package_json', 'governmentAuthor1') }} as government_author1,
        {{ json_text('package_json', 'governmentAuthor2') }} as government_author2,
        {{ json_text('package_json', 'publisher') }} as publisher,
        {{ json_text('package_json', 'collectionCode') }} as collection_code,
        {{ json_text('package_json', 'otherIdentifier.migrated-doc-id') }} as migrated_doc_id,
        {{ json_text('package_json', 'suDocClassNumber') }} as su_doc_class_number,
        {{ json_text('package_json', 'suDocItemNumber') }} as su_doc_item_number,
        {{ json_text('package_json', 'otherSuDocClassNumber') }} as other_su_doc_class_number,
        {{ json_timestamptz('package_json', 'lastModified') }} as last_modified
    from normalized
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    {{ govinfo_committeereport_report_id('report_set_id', 'report_part', 'is_errata') }} as report_id,
    report_set_id,
    package_id,
    report_type,
    report_number,
    report_part,
    title,
    subtitle,
    chamber,
    congress,
    session,
    pages,
    is_errata,
    issued_at,
    branch,
    government_author1,
    government_author2,
    publisher,
    collection_code,
    migrated_doc_id,
    su_doc_class_number,
    su_doc_item_number,
    other_su_doc_class_number,
    last_modified
-- The GPO-J6-REPORT package is a standalone investigative publication that
-- was harvested through the broad GovInfo report route, not a committee
-- report package.  It has no report-set identity.  Keep the immutable raw
-- record, but exclude it from the committee-report source relation so the
-- source contract cannot manufacture null report identifiers.
from typed
where report_set_id is not null
