{{ config(tags=["committee_report_reconciliation", "intermediate"]) }}

/*
   GovInfo has two useful report collections here:

   * CRPT is the modern collection and is the preferred representative when
     both sources expose the same report.
   * SERIALSET is the historical archive. Its serialSetNumber is a catalog
     number, not a report number, so staging identity uses documentNumber and
     congress only.

   Keep every source package/part in this relation for a future public union.
   Candidate matching consumes only one representative per canonical report
   identity; duplicate SERIALSET packages remain evidence without creating
   many-to-one matches or silently discarding history.
*/
with source_rows as (
    select
        reports.report_id,
        'govinfo:' || reports.package_id || ':' || reports.report_part::text
            || case when coalesce(reports.is_errata, false) then 'e' else '' end as source_record_id,
        'govinfo' as source_name,
        reports.raw_id as source_raw_id,
        reports.raw_response_version_id as source_raw_response_version_id,
        reports.endpoint_url as source_endpoint_url,
        reports.package_id,
        serialset.serialset_number as serial_set_number,
        {{ committee_report_canonical_id(
            'reports.report_type', 'reports.report_number', 'reports.congress', 'reports.report_part', 'reports.is_errata'
        ) }} as canonical_report_id,
        lower(reports.report_type) as report_type,
        reports.report_number,
        reports.congress,
        reports.report_part,
        reports.is_errata,
        reports.title,
        {{ normalize_match_text('reports.title') }} as normalized_title,
        {{ normalize_chamber('reports.chamber') }} as chamber,
        case
            -- These GPO-CRPT packages were fetched through the broad route,
            -- but their package IDs are still unambiguous CRPT identities.
            when lower(reports.package_id) ~ '^gpo-crpt-[0-9]+[a-z]+[0-9]+$' then 'CRPT'
            else upper(reports.collection_code)
        end as collection_code,
        reports.issued_at,
        reports.last_modified as source_updated_at
    from {{ ref('stg_govinfo__committeereports') }} as reports
    left join {{ ref('stg_govinfo__committeereports_serialset') }} as serialset
      on serialset.package_id = reports.package_id
    where lower(reports.report_type) in ('hrpt', 'srpt', 'erpt', 'ccrpt')
      and (
          upper(coalesce(reports.collection_code, '')) in ('CRPT', 'SERIALSET')
          or lower(reports.package_id) ~ '^gpo-crpt-[0-9]+[a-z]+[0-9]+$'
      )
      and nullif(trim(reports.package_id), '') is not null
      and reports.report_number is not null
      and reports.report_number > 0
      and reports.congress is not null
      and reports.report_part is not null
),
package_parts as (
    select
        source_rows.*,
        row_number() over (
            partition by package_id, report_part, is_errata
            order by source_updated_at desc nulls last,
                     normalized_title desc nulls last,
                     source_raw_id desc
        ) as package_part_rank
    from source_rows
),
ranked as (
    select
        package_parts.*,
        row_number() over (
            partition by canonical_report_id
            order by
                case when collection_code = 'CRPT' then 0 else 1 end,
                source_updated_at desc nulls last,
                package_id desc,
                source_raw_id desc
        ) as representative_rank
    from package_parts
    where package_part_rank = 1
)

select
    report_id,
    source_record_id,
    source_name,
    source_raw_id,
    source_raw_response_version_id,
    source_endpoint_url,
    package_id,
    serial_set_number,
    canonical_report_id,
    report_type,
    report_number,
    congress,
    report_part,
    is_errata,
    title,
    normalized_title,
    chamber,
    collection_code,
    issued_at,
    source_updated_at,
    case
        when collection_code = 'SERIALSET' then 'historical_serialset'
        else 'crpt'
    end as identity_status,
    representative_rank = 1 as is_representative_package
from ranked
