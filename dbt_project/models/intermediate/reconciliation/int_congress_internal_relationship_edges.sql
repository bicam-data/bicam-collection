/* The member-bill source expands roughly eight million rows. PostgreSQL
   otherwise chooses external sort/unique plans under the cluster's
   conservative 16 MB default, turning this bounded rebuild into a
   multi-minute disk-I/O job. Keep the memory/plan hint local to this model
   so other dbt relations and services are unaffected. */
{{ config(
    materialized="table",
    pre_hook=[
        "set local work_mem='2GB'",
        "set local enable_sort='off'",
        "set local max_parallel_workers_per_gather=4"
    ],
    tags=["operations", "quality", "congress_internal_reconciliation"]
) }}

-- Keep each relationship in a source-neutral (left_id, right_id) shape while
-- retaining which API direction supplied the edge.  Rebuild every supported
-- family on each run so newly crawled source rows cannot be omitted from the
-- reciprocity union.  This is deliberately a table rather than an append-only
-- incremental model: source-side corrections and newly observed counterparts
-- must be reflected in both directions.
with member_bill_edges as (
    select distinct 'member_bill_sponsor' as relationship_type, bioguide_id as left_id, bill_id as right_id, 'member' as left_source, 'left' as edge_side
    from {{ ref('stg_congress__member_bills') }}
    where sponsorship_type = 'sponsor' and bioguide_id is not null and bill_id is not null
    union all
    select distinct 'member_bill_sponsor', bioguide_id, bill_id, 'bill', 'right'
    from {{ ref('stg_congress__bill_sponsors') }}
    where bioguide_id is not null and bill_id is not null
), member_bill_cosponsor_edges as (
    select distinct 'member_bill_cosponsor' as relationship_type, bioguide_id, bill_id, 'member', 'left'
    from {{ ref('stg_congress__member_bills') }}
    where sponsorship_type = 'cosponsor' and bioguide_id is not null and bill_id is not null
    union all
    select distinct 'member_bill_cosponsor', bioguide_id, bill_id, 'bill', 'right'
    from {{ ref('stg_congress__bill_cosponsors') }}
    where bioguide_id is not null and bill_id is not null
), committee_bill_edges as (
    select distinct 'committee_bill' as relationship_type, committee_id as left_id, bill_id as right_id, 'committee', 'left'
    from {{ ref('stg_congress__committee_bills') }}
    where committee_id is not null and bill_id is not null
    union all
    select distinct 'committee_bill', committee_code, bill_id, 'bill', 'right'
    from {{ ref('stg_congress__bill_committees') }}
    where committee_code is not null and bill_id is not null
), bill_committee_report_edges as (
    select distinct 'bill_committee_report' as relationship_type,
        bill_id as left_id, report_part_id as right_id, 'bill' as left_source, 'left' as edge_side
    from {{ ref('stg_congress__bill_committeereports') }}
    where bill_id is not null and report_part_id is not null
    union all
    select distinct 'bill_committee_report', bill_id, report_part_id, 'committee_report', 'right'
    from {{ ref('stg_congress__committee_report_bills') }}
    where bill_id is not null and report_part_id is not null
), bill_amendment_edges as (
    /* Bills expose their amendments, and amendment details expose the bill
       they amend. Keep both directions in the same canonical bill→amendment
       shape so a one-sided API response is visible without losing the edge. */
    select distinct 'bill_amendment' as relationship_type,
        bill_id as left_id, amendment_id as right_id,
        'bill' as left_source, 'left' as edge_side
    from {{ ref('stg_congress__bill_amendments') }}
    where bill_id is not null and amendment_id is not null
    union all
    select distinct 'bill_amendment', bill_id, amendment_id,
        'amendment', 'right'
    from {{ ref('stg_congress__amendment_amended_bills') }}
    where bill_id is not null and amendment_id is not null
), committee_nomination_edges as (
    select distinct 'committee_nomination' as relationship_type,
        committee_id as left_id, nomination_id as right_id, 'committee' as left_source, 'left' as edge_side
    from {{ ref('stg_congress__committee_nominations') }}
    where committee_id is not null and nomination_id is not null
    union all
    select distinct 'committee_nomination', committee_id, nomination_part_id, 'nomination', 'right'
    from {{ ref('stg_congress__nomination_committees') }}
    where committee_id is not null and nomination_part_id is not null
), treaty_committee_report_edges as (
    select distinct 'treaty_committee_report' as relationship_type,
        treaty_id as left_id, report_id as right_id, 'treaty' as left_source, 'left' as edge_side
    from {{ ref('stg_congress__treaty_committeereports') }}
    where treaty_id is not null and report_id is not null
    union all
    select distinct 'treaty_committee_report', treaty_id, report_id, 'committee_report', 'right'
    from {{ ref('stg_congress__committee_report_treaties') }}
    where treaty_id is not null and report_id is not null
), committee_committee_report_edges as (
    select distinct 'committee_committee_report' as relationship_type,
        committee_id as left_id, report_part_id as right_id,
        'committee' as left_source, 'left' as edge_side
    from {{ ref('stg_congress__committee_reports') }}
    where committee_id is not null and report_part_id is not null
    union all
    select distinct 'committee_committee_report', committee_id, report_part_id,
        'committee_report', 'right'
    from {{ ref('stg_congress__committee_report_committees') }}
    where committee_id is not null and report_part_id is not null
), committee_meeting_hearing_edges as (
    /*
       Committee-meeting payloads expose hearingTranscript children, while a
       Congress.gov hearing detail may expose the inverse associatedMeeting
       edge.  Normalize both to one meeting -> hearing shape so a missing
       inverse is visible without manufacturing a second chamber hearing.
    */
    select distinct
        'committee_meeting_hearing' as relationship_type,
        source_id as left_id,
        target_id as right_id,
        'committee_meeting' as left_source,
        'left' as edge_side
    from {{ ref('int_hearing_committee_meeting_links') }}
    where relationship_type = 'committee_meeting_to_congress_hearing_child'
      and source_system = 'congress'
      and target_system = 'congress'
    union all
    select distinct
        'committee_meeting_hearing',
        target_id,
        source_id,
        'hearing',
        'right'
    from {{ ref('int_hearing_committee_meeting_links') }}
    where relationship_type = 'congress_hearing_to_committee_meeting_associated'
      and source_system = 'congress'
      and target_system = 'congress'
)
-- Each source-side CTE is already distinct, and relationship types are
-- disjoint.  Avoid a second global DISTINCT over millions of raw-backed rows.
select relationship_type, left_id, right_id, left_source, edge_side
from (
    select * from member_bill_edges
    union all select * from member_bill_cosponsor_edges
    union all select * from committee_bill_edges
    union all select * from bill_amendment_edges
    union all select * from bill_committee_report_edges
    union all select * from committee_nomination_edges
    union all select * from treaty_committee_report_edges
    union all select * from committee_committee_report_edges
    union all select * from committee_meeting_hearing_edges
) as all_edges
