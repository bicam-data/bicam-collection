{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committee_reports'],
    indexes=[
      {'columns': ['report_id'], 'unique': true},
      {'columns': ['congress', 'report_type', 'report_number']}
    ]
) }}

/*
   The public report grain is the repaired, part-aware canonical identity.
   Congress.gov remains the descriptive authority when it exists; GovInfo
   CRPT and SERIALSET records fill unmatched identities and source-null
   fields.  Keep the full GovInfo source relation in the union before ranking
   so a SERIALSET-only report cannot disappear merely because another package
   has the same printed number.
*/
with congress_records as (
    select
        reports.canonical_report_id as report_id,
        reports.report_type,
        reports.report_number,
        reports.report_part,
        reports.congress,
        details.session,
        coalesce(details.title, reports.title) as title,
        null::text as subtitle,
        {{ normalize_chamber('coalesce(details.chamber, reports.chamber)') }} as chamber,
        reports.is_errata,
        details.citation,
        details.is_conference_report,
        coalesce(details.issued_at, reports.issued_at)::date as issued_at,
        null::integer as pages,
        details.texts_count::integer as texts_count,
        coalesce(details.updated_at, reports.source_updated_at)::timestamptz as updated_at,
        reports.source_record_id,
        'congress'::text as source_name,
        case
            when details.report_part_id is not null then 0
            else 2
        end as source_priority
    from {{ ref('int_committee_reports__congress') }} as reports
    left join {{ ref('stg_congress__committee_report_detail') }} as details
      on details.report_part_id = reports.canonical_report_id
    where reports.canonical_report_id is not null
), govinfo_records as (
    select
        reports.canonical_report_id as report_id,
        reports.report_type,
        reports.report_number,
        reports.report_part,
        reports.congress,
        packages.session,
        reports.title,
        packages.subtitle,
        {{ normalize_chamber('reports.chamber') }} as chamber,
        reports.is_errata,
        null::text as citation,
        null::boolean as is_conference_report,
        reports.issued_at::date as issued_at,
        packages.pages::integer as pages,
        null::integer as texts_count,
        reports.source_updated_at::timestamptz as updated_at,
        reports.source_record_id,
        'govinfo'::text as source_name,
        case when reports.collection_code = 'CRPT' then 1 else 2 end as source_priority
    from {{ ref('int_committee_reports__govinfo') }} as reports
    left join {{ ref('stg_govinfo__committeereports_packages') }} as packages
      on packages.package_id = reports.package_id
    where reports.canonical_report_id is not null
), source_union as (
    select * from congress_records
    union all
    select * from govinfo_records
), reconciliation_status as (
    /* Force this root to remain coupled to the candidate/match relation.  The
       source union above intentionally retains unmatched rows; this CTE only
       supplies reconciliation evidence for deterministic tie-breaking. */
    select
        matches.canonical_report_id as report_id,
        bool_or(matches.decision = 'accepted') as has_accepted_match,
        bool_or(matches.congress_source_record_id is not null) as has_congress_match,
        bool_or(matches.govinfo_source_record_id is not null) as has_govinfo_match
    from {{ ref('fct_committee_report_matches') }} as matches
    group by matches.canonical_report_id
), ranked as (
    select
        source_union.*,
        coalesce(reconciliation_status.has_accepted_match, false) as has_accepted_match,
        row_number() over (
            partition by source_union.report_id
            order by source_union.source_priority,
                     (source_union.source_name = 'congress') desc,
                     source_union.updated_at desc nulls last,
                     source_union.source_record_id desc
        ) as _rn
    from source_union
    left join reconciliation_status
      on reconciliation_status.report_id = source_union.report_id
)
select
    report_id::text as report_id,
    report_type::text as report_type,
    report_number::integer as report_number,
    report_part::integer as report_part,
    congress::integer as congress,
    session::integer as session,
    title::text as title,
    subtitle::text as subtitle,
    {{ normalize_chamber('chamber') }}::text as chamber,
    citation::text as citation,
    is_conference_report::boolean as is_conference_report,
    issued_at::date as issued_at,
    pages::integer as pages,
    texts_count::integer as texts_count,
    updated_at::timestamptz as updated_at
from ranked
where _rn = 1
  and report_id is not null
  and report_type is not null
  and report_number is not null
  and congress is not null
