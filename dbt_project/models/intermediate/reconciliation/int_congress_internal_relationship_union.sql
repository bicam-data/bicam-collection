{{ config(
    materialized="table",
    tags=["operations", "quality", "congress_internal_reconciliation"]
) }}

/* Source-neutral relationship set. A relationship observed on either
   Congress.gov direction is retained once, with side-presence flags so both
   downstream categories use the same canonical union. */
with all_edges as (
    select relationship_type, left_id, right_id, left_source, edge_side
    from {{ ref('int_congress_internal_relationship_edges') }}
    union all
    select relationship_type, left_id, right_id, left_source, edge_side
    from {{ ref('int_congress_member_amendment_relationship_edges') }}
)
select
    relationship_type,
    left_id,
    right_id,
    bool_or(edge_side = 'left') as left_present,
    bool_or(edge_side = 'right') as right_present,
    array_agg(distinct edge_side order by edge_side) as evidence_directions,
    array_agg(distinct left_source order by left_source) as evidence_sources,
    count(*)::integer as evidence_count
from all_edges
group by 1, 2, 3
