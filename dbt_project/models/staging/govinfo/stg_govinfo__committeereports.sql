{{ config(tags=["govinfo_committee_reports", "staging"]) }}

with package_reports as (
    select
        raw_id as package_raw_id,
        endpoint_url as package_endpoint_url,
        raw_response_version_id as package_raw_response_version_id,
        report_id,
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
    from {{ ref('stg_govinfo__committeereports_packages') }}
),
granule_detail as (
    select
        id as raw_id,
        endpoint_url,
        raw_response_version_id,
        response as granule_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'govinfo'
      and category = 'govinfo-committee-reports'
      and node_type = 'govinfo_committee_report_granule_detail'
),
granule_reports as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        {{ json_text('granule_json', 'packageId') }} as package_id,
        {{ json_text('granule_json', 'granuleId') }} as granule_id,
        packages.report_set_id as report_set_id,
        coalesce(
            lower({{ json_text('granule_json', 'docClass') }}),
            packages.report_type
        ) as report_type,
        packages.report_number as report_number,
        {{ govinfo_committeereport_report_part(
            json_int('granule_json', 'partNumber'),
            json_text('granule_json', 'heading'),
            json_text('granule_json', 'granuleId'),
            'packages.congress',
            json_text('granule_json', 'granuleClass')
        ) }} as report_part,
        {{ json_text('granule_json', 'title') }} as title,
        {{ json_text('granule_json', 'heading') }} as heading,
        packages.subtitle as subtitle,
        packages.chamber as chamber,
        packages.congress as congress,
        packages.session as session,
        coalesce({{ json_int('granule_json', 'pages') }}, packages.pages) as pages,
        {{ govinfo_committeereport_is_errata(
            "coalesce(" ~ json_text('granule_json', 'title') ~ ", packages.title)",
            json_text('granule_json', 'heading'),
            json_text('granule_json', 'granuleId')
        ) }} as is_errata,
        coalesce({{ json_date('granule_json', 'dateIssued') }}, packages.issued_at) as issued_at,
        packages.branch as branch,
        packages.government_author1 as government_author1,
        packages.government_author2 as government_author2,
        packages.publisher as publisher,
        coalesce({{ json_text('granule_json', 'collectionCode') }}, packages.collection_code) as collection_code,
        packages.migrated_doc_id as migrated_doc_id,
        packages.su_doc_class_number as su_doc_class_number,
        packages.su_doc_item_number as su_doc_item_number,
        packages.other_su_doc_class_number as other_su_doc_class_number,
        coalesce({{ json_timestamptz('granule_json', 'lastModified') }}, packages.last_modified) as last_modified
    from granule_detail
    left join package_reports packages
      on packages.package_id = {{ json_text('granule_json', 'packageId') }}
),
finalized_granules as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        {{ govinfo_committeereport_report_id('report_set_id', 'report_part', 'is_errata') }} as report_id,
        report_set_id,
        package_id,
        granule_id,
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
    from granule_reports
),
package_only as (
    select
        package_raw_id as raw_id,
        package_endpoint_url as endpoint_url,
        package_raw_response_version_id as raw_response_version_id,
        report_id,
        report_set_id,
        package_id,
        cast(null as text) as granule_id,
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
    from package_reports
    where not exists (
        select 1
        from finalized_granules granules
        where granules.package_id = package_reports.package_id
    )
)

select * from finalized_granules
union all
select * from package_only
