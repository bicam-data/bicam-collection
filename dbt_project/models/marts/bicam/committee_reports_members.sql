{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committee_reports'],
    indexes=[
      {'columns': ['report_id', 'bioguide_id'], 'unique': true},
      {'columns': ['bioguide_id']}
    ]
) }}

/* Publish only evidence-backed identities. Composite member strings are
   represented by their independently resolved component identities. */
with report_granules as (
    select distinct
        granules.package_id,
        granules.granule_id,
        reports.canonical_report_id as report_id
    from {{ ref('stg_govinfo__committeereports_granules') }} as granules
    join {{ ref('int_committee_reports__govinfo') }} as reports
      on reports.package_id = granules.package_id
     and reports.report_part = granules.report_part
     and reports.is_errata = granules.is_errata
    where reports.canonical_report_id is not null
),
direct_members as (
    select
        report_granules.report_id,
        member_refs.linkable_bioguide_id as bioguide_id
    from {{ ref('bridge_govinfo__member_references') }} as member_refs
    join report_granules using (package_id, granule_id)
    where member_refs.category = 'govinfo-committee-reports'
      and member_refs.linkable_bioguide_id is not null
),
component_members as (
    select
        report_granules.report_id,
        components.resolved_bioguide_id as bioguide_id
    from {{ ref('bridge_govinfo__member_reference_components') }} as components
    join report_granules using (package_id, granule_id)
    where components.category = 'govinfo-committee-reports'
      and components.resolved_bioguide_id is not null
),
all_members as (
    select * from direct_members
    union all
    select * from component_members
)

select distinct all_members.report_id::text, all_members.bioguide_id::text
from all_members
join {{ ref('members') }} as members using (bioguide_id)
where all_members.report_id is not null
  and all_members.bioguide_id is not null
