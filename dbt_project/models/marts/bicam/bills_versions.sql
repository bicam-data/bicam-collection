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
    case
        when package_id ~* 'v[0-9]+[a-z]+$'
        then upper((regexp_match(lower(package_id), 'v[0-9]+([a-z]+)$'))[1])
        else upper(bill_version)
    end as version_code,
    case
        when package_id ~* 'v[0-9]+[a-z]+$'
        then (regexp_match(lower(package_id), 'v([0-9]+)[a-z]+$'))[1]::integer
        when bill_version ~ '^[0-9]+$' then bill_version::integer
        else null
    end as version_variant,
    nullif(raw.response #>> '{download,txtLink}', '') as formatted_text,
    nullif(html.text_content, '') as raw_text,
    package_title as title,
    package_short_title as short_title,
    issued_at,
    last_modified,
    is_representative_package
from {{ ref('bridge_govinfo__bill_versions') }} as versions
left join {{ ref('int_raw_responses__current') }} as raw
  on raw.raw_id = versions.govinfo_raw_id
left join public.html_text_successful_current as html
  on html.url = nullif(raw.response #>> '{download,txtLink}', '')
 and html.extraction_status = 'success'
