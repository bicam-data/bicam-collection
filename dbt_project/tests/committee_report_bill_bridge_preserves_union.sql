/* The release-facing bridge must preserve every unioned edge and both
   direction flags from the source-neutral intermediate relation. */
with expected as (
    select
        link_id,
        from_bill_side,
        from_report_side
    from {{ ref('int_committee_report_bill_links') }}
), actual as (
    select
        link_id,
        from_bill_side,
        from_report_side
    from {{ ref('bridge_bills__committee_reports') }}
)
select
    coalesce(expected.link_id, actual.link_id) as link_id
from expected
full outer join actual using (link_id)
where expected.link_id is null
   or actual.link_id is null
   or expected.from_bill_side <> actual.from_bill_side
   or expected.from_report_side <> actual.from_report_side
