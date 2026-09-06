-- One-time migration for the historical internal-reciprocity edge table.
-- The original table contains the member↔bill and committee↔bill edges.
-- This appends the smaller report↔bill and nomination↔committee directions
-- without reparsing the multi-million-row member amendment endpoints.
alter table public.int_congress_internal_relationship_edges
    add column if not exists edge_side text;

-- The pre-existing table predates the side-presence contract.  These source
-- labels make the migration deterministic for its five legacy relationship
-- families; new rows below carry edge_side explicitly.
update public.int_congress_internal_relationship_edges
set edge_side = case
    when left_source in ('member', 'committee') then 'left'
    when left_source = 'bill' then 'right'
    else edge_side
end
where edge_side is null;

insert into public.int_congress_internal_relationship_edges
    (relationship_type, left_id, right_id, left_source, edge_side)
with bill_committee_report_edges as (
    select distinct
        'bill_committee_report' as relationship_type,
        bill_id as left_id,
        report_part_id as right_id,
        'bill' as left_source,
        'left' as edge_side
    from public.stg_congress__bill_committeereports
    where bill_id is not null and report_part_id is not null
    union all
    select distinct
        'bill_committee_report',
        bill_id,
        report_part_id,
        'committee_report',
        'right'
    from public.stg_congress__committee_report_bills
    where bill_id is not null and report_part_id is not null
), committee_nomination_edges as (
    select distinct
        'committee_nomination' as relationship_type,
        committee_id as left_id,
        nomination_id as right_id,
        'committee' as left_source,
        'left' as edge_side
    from public.stg_congress__committee_nominations
    where committee_id is not null and nomination_id is not null
    union all
    select distinct
        'committee_nomination',
        committee_id,
        nomination_part_id,
        'nomination',
        'right'
    from public.stg_congress__nomination_committees
    where committee_id is not null and nomination_part_id is not null
)
select relationship_type, left_id, right_id, left_source, edge_side
from bill_committee_report_edges
union all
select relationship_type, left_id, right_id, left_source, edge_side
from committee_nomination_edges;
