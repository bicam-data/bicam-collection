/* A bill's committee-reports child and a committee report's associated-bills
   child are two evidence directions for the same Congress.gov relationship.
   The source-neutral link model must retain every edge and the side on which
   it was observed so consumers of either category see the union. */
with expected as (
    select
        bill_id,
        report_part_id,
        bool_or(evidence_side = 'bill') as from_bill_side,
        bool_or(evidence_side = 'report') as from_report_side
    from (
        select distinct
            bill_id,
            report_part_id,
            'bill' as evidence_side
        from {{ ref('stg_congress__bill_committeereports') }}
        where bill_id is not null
          and report_part_id is not null
        union all
        select distinct
            bill_id,
            report_part_id,
            'report' as evidence_side
        from {{ ref('stg_congress__committee_report_bills') }}
        where bill_id is not null
          and report_part_id is not null
    ) as evidence
    group by bill_id, report_part_id
), actual as (
    select
        bill_id,
        report_part_id,
        bool_or(from_bill_side) as from_bill_side,
        bool_or(from_report_side) as from_report_side,
        count(*) as row_count
    from {{ ref('int_committee_report_bill_links') }}
    group by bill_id, report_part_id
)
select
    coalesce(expected.bill_id, actual.bill_id) as bill_id,
    coalesce(expected.report_part_id, actual.report_part_id) as report_part_id
from expected
full outer join actual using (bill_id, report_part_id)
where expected.bill_id is null
   or actual.bill_id is null
   or actual.row_count <> 1
   or coalesce(expected.from_bill_side, false) <> coalesce(actual.from_bill_side, false)
   or coalesce(expected.from_report_side, false) <> coalesce(actual.from_report_side, false)
