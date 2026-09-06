{{ config(
    pre_hook=[
        "set local work_mem='2GB'",
        "set local enable_sort='off'",
        "set local max_parallel_workers_per_gather=4"
    ]
) }}

/* A bill can expose report part 1 while the report-side endpoint exposes
   part 2 (or vice versa).  The report base identity is the same; exact part
   IDs remain distinct and the difference is classified as source asymmetry. */
with part_edges as (
    select distinct
        edges.edge_side,
        edges.left_id as bill_id,
        edges.right_id as report_part_id,
        report_detail.report_type,
        report_detail.report_number,
        report_detail.congress
    from {{ ref('int_congress_internal_relationship_edges') }} as edges
    join {{ ref('stg_congress__committee_report_detail') }} as report_detail
      on report_detail.report_part_id = edges.right_id
    where edges.relationship_type = 'bill_committee_report'
), bases as (
    select
        bill_id,
        report_type,
        report_number,
        congress,
        bool_or(edge_side = 'left') as left_base_present,
        bool_or(edge_side = 'right') as right_base_present,
        count(distinct report_part_id) as part_count
    from part_edges
    group by bill_id, report_type, report_number, congress
), expected as (
    select distinct parts.bill_id, parts.report_part_id
    from part_edges as parts
    join bases
      on bases.bill_id = parts.bill_id
     and bases.report_type = parts.report_type
     and bases.report_number = parts.report_number
     and bases.congress = parts.congress
    where bases.part_count > 1
      and (
        (parts.edge_side = 'left' and bases.right_base_present)
        or (parts.edge_side = 'right' and bases.left_base_present)
      )
)
select
    expected.bill_id,
    expected.report_part_id,
    reciprocity.reciprocity_status
from expected
left join {{ ref('fct_congress_internal_reciprocity') }} as reciprocity
  on reciprocity.relationship_type = 'bill_committee_report'
 and reciprocity.left_id = expected.bill_id
 and reciprocity.right_id = expected.report_part_id
where reciprocity.reciprocity_status not in (
    'left_only_asymmetric_source',
    'right_only_asymmetric_source',
    'reciprocal'
)
   or reciprocity.reciprocity_status is null
