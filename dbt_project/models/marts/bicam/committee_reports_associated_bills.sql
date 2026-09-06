{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committee_reports'],
    indexes=[
      {'columns': ['report_id', 'bill_id'], 'unique': true},
      {'columns': ['bill_id']}
    ]
) }}

/* Congress.gov exposes this edge from both bill and report endpoints.  The
   internal relationship union is the source-neutral reciprocal set; GovInfo
   references add valid historical edges absent from either endpoint. */
with congress_edges as (
    select distinct
        reports.canonical_report_id as report_id,
        edges.left_id as bill_id
    from {{ ref('int_congress_internal_relationship_union') }} as edges
    join {{ ref('int_committee_reports__congress') }} as reports
      on reports.canonical_report_id = edges.right_id
    join {{ ref('bills') }} as bills
      on bills.bill_id = edges.left_id
    where edges.relationship_type = 'bill_committee_report'
      and edges.left_id is not null
      and edges.right_id is not null
      and reports.canonical_report_id is not null
), govinfo_edges as (
    select distinct
        reports.canonical_report_id as report_id,
        gov_refs.bill_id
    from {{ ref('stg_govinfo__committeereports_reference_bills') }} as gov_refs
    join {{ ref('int_committee_reports__govinfo') }} as reports
      on reports.package_id = gov_refs.package_id
     and reports.report_id = gov_refs.report_id
    join {{ ref('bills') }} as bills
      on bills.bill_id = gov_refs.bill_id
    where gov_refs.bill_id is not null
      and reports.canonical_report_id is not null
)
select distinct report_id::text as report_id, bill_id::text as bill_id
from (
    select * from congress_edges
    union all
    select * from govinfo_edges
) as edges
where report_id is not null
  and bill_id is not null
