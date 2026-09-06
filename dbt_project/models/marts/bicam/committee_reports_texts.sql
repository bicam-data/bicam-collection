{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committee_reports'],
    indexes=[{'columns': ['report_id'], 'unique': true}]
) }}

with congress_texts as (
    select
        reports.canonical_report_id as report_id,
        texts.raw_text,
        texts.formatted_text,
        texts.formatted_text_is_errata,
        texts.pdf,
        texts.pdf_is_errata,
        2 as source_priority
    from {{ ref('stg_congress__committee_report_texts') }} as texts
    join {{ ref('int_committee_reports__congress') }} as reports
      on reports.canonical_report_id = texts.report_part_id
    where reports.canonical_report_id is not null
), govinfo_granule_texts as (
    /* Download URLs are locators, not document contents. Keep the text
       columns null unless content was actually harvested, and expose the
       official PDF locator in the contract's PDF field. */
    select
        reports.canonical_report_id as report_id,
        null::text as raw_text,
        null::text as formatted_text,
        null::boolean as formatted_text_is_errata,
        nullif(raw.response #>> '{download,pdfLink}', '') as pdf,
        null::boolean as pdf_is_errata,
        1 as source_priority
    from {{ ref('stg_govinfo__committeereports_granules') }} as granules
    join {{ ref('int_committee_reports__govinfo') }} as reports
      on reports.package_id = granules.package_id
     and reports.report_id = granules.report_id
    join {{ ref('int_raw_responses__current') }} as raw
      on raw.source = 'govinfo'
     and raw.category = 'govinfo-committee-reports'
     and raw.node_type = 'govinfo_committee_report_granule_detail'
     and raw.response #>> '{packageId}' = granules.package_id
     and raw.response #>> '{granuleId}' = granules.granule_id
    where reports.canonical_report_id is not null
), govinfo_package_texts as (
    /* Most CRPT packages expose their official PDF on the package detail,
       including packages for which GovInfo has not published a corresponding
       granule detail. Preserve that locator as independent evidence while
       continuing to prefer Congress.gov renditions and granule-level links. */
    select
        reports.canonical_report_id as report_id,
        null::text as raw_text,
        null::text as formatted_text,
        null::boolean as formatted_text_is_errata,
        nullif(raw.response #>> '{download,pdfLink}', '') as pdf,
        null::boolean as pdf_is_errata,
        0 as source_priority
    from {{ ref('int_committee_reports__govinfo') }} as reports
    join {{ ref('int_raw_responses__current') }} as raw
      on raw.raw_id = reports.source_raw_id
    where reports.canonical_report_id is not null
), candidates as (
    select * from congress_texts
    union all
    select * from govinfo_granule_texts
    union all
    select * from govinfo_package_texts
), ranked as (
    select
        candidates.*,
        row_number() over (
            partition by report_id
            order by (raw_text is not null or formatted_text is not null or pdf is not null) desc,
                     source_priority desc,
                     raw_text desc nulls last
        ) as _rn
    from candidates
)
select
    report_id::text as report_id,
    coalesce(nullif(raw_text, ''), html.text_content)::text as raw_text,
    formatted_text::text as formatted_text,
    formatted_text_is_errata::boolean as formatted_text_is_errata,
    pdf::text as pdf,
    pdf_is_errata::boolean as pdf_is_errata
from ranked
left join public.html_text_successful_current as html
  on html.url = ranked.formatted_text
 and html.extraction_status = 'success'
where _rn = 1
