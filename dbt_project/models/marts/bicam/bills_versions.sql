{{ config(
    schema='bicam',
    materialized='table',
    tags=['bills', 'versions', 'public'],
    indexes=[
      {'columns': ['govinfo_package_id'], 'unique': true},
      {'columns': ['bill_id']},
      {'columns': ['bill_id', 'version_code']}
    ]
) }}

/*
   Supplemental, version-grain bill relation.  The core bills table keeps one
   representative GovInfo package for compatibility; this table preserves
   every BILLS package so consumers can follow version history without having
   to join internal reconciliation tables.
*/
select
    canonical_bill_id as bill_id,
    package_id as govinfo_package_id,
    /*
       Most BILLS packages expose the semantic code directly (IH, RH, ENR,
       ...).  A small legacy set instead uses package IDs such as
       BILLS-108hr10v1rh and reports only the numeric portion in
       billVersionExtended.  Use the alphabetic package suffix as the
       semantic code in that case, while retaining the numeric variant below.
    */
    coalesce(
        {{ govinfo_bill_version_code(govinfo_bill_package_suffix('package_id')) }},
        upper(bill_version)
    ) as version_code,
    coalesce(
        {{ govinfo_bill_version_variant(govinfo_bill_package_suffix('package_id')) }},
        case when bill_version ~ '^[0-9]+$' then bill_version::integer end
    ) as version_variant,
    {{ govinfo_bill_package_suffix('package_id') }} as rendition_suffix,
    {{ govinfo_bill_starprint_number(govinfo_bill_package_suffix('package_id')) }} as starprint_number,
    nullif(raw.response #>> '{download,txtLink}', '') as formatted_text,
    nullif(html.text_content, '') as raw_text,
    package_title as title,
    package_short_title as short_title,
    issued_at,
    last_modified,
    is_representative_package
from {{ ref('bridge_govinfo__bill_versions') }} as versions
/* Never substitute a later payload for the bridge's source version. Missing
   historical evidence leaves renditions NULL while preserving the package.
   Query the immutable archive directly: a stale incremental copy must not
   force another full raw-model refresh to recover this exact version. */
left join {{ source('raw', 'raw_response_versions') }} as raw
  on raw.id = versions.govinfo_raw_response_version_id
 and raw.raw_response_id = versions.govinfo_raw_id
left join public.html_text_successful_current as html
  on html.url = nullif(raw.response #>> '{download,txtLink}', '')
 and html.extraction_status = 'success'
