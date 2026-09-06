/* The canonical union must retain exactly the source-side direction flags and
   provenance that produced each relationship. This makes unioned edges safe
   to use in both endpoint-specific downstream categories. */
with expected as (
    select
        relationship_type,
        left_id,
        right_id,
        bool_or(edge_side = 'left') as left_present,
        bool_or(edge_side = 'right') as right_present,
        array_agg(distinct edge_side order by edge_side) as evidence_directions,
        array_agg(distinct left_source order by left_source) as evidence_sources,
        count(*)::integer as evidence_count
    from (
        select relationship_type, left_id, right_id, left_source, edge_side
        from {{ ref('int_congress_internal_relationship_edges') }}
        union all
        select relationship_type, left_id, right_id, left_source, edge_side
        from {{ ref('int_congress_member_amendment_relationship_edges') }}
    ) as edges
    group by 1, 2, 3
), actual as (
    select
        relationship_type,
        left_id,
        right_id,
        left_present,
        right_present,
        evidence_directions,
        evidence_sources,
        evidence_count
    from {{ ref('int_congress_internal_relationship_union') }}
)
select
    coalesce(expected.relationship_type, actual.relationship_type) as relationship_type,
    coalesce(expected.left_id, actual.left_id) as left_id,
    coalesce(expected.right_id, actual.right_id) as right_id
from expected
full outer join actual using (relationship_type, left_id, right_id)
where expected.relationship_type is null
   or actual.relationship_type is null
   or expected.left_present <> actual.left_present
   or expected.right_present <> actual.right_present
   or expected.evidence_directions <> actual.evidence_directions
   or expected.evidence_sources <> actual.evidence_sources
   or expected.evidence_count <> actual.evidence_count
