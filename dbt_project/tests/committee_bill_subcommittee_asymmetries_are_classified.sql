{{ config(
    pre_hook=[
        "set local work_mem='2GB'",
        "set local enable_sort='off'",
        "set local max_parallel_workers_per_gather=4"
    ]
) }}

/* A committee-side referral to a subcommittee is often represented on the
   bill side by the parent committee. The IDs stay distinct; only the
   reciprocity classification is normalized to source asymmetry. Use the
   materialized relationship edge table here so this regression does not
   repeatedly parse millions of raw bill-committee payloads. */
with hierarchy as (
    select distinct committee_id as parent_id, subcommittee_id as child_id
    from {{ ref('stg_congress__committee_subcommittees') }}
    where committee_id is not null
      and subcommittee_id is not null
), parent_edges as (
    select distinct left_id as parent_id, right_id as bill_id
    from {{ ref('int_congress_internal_relationship_edges') }}
    where relationship_type = 'committee_bill'
      and edge_side = 'right'
      and left_id is not null
      and right_id is not null
)
select
    reciprocity.left_id,
    reciprocity.right_id,
    reciprocity.reciprocity_status
from {{ ref('fct_congress_internal_reciprocity') }} as reciprocity
join hierarchy
  on hierarchy.child_id = reciprocity.left_id
join parent_edges
  on parent_edges.parent_id = hierarchy.parent_id
 and parent_edges.bill_id = reciprocity.right_id
where reciprocity.relationship_type = 'committee_bill'
  and reciprocity.left_present
  and not reciprocity.right_present
  and reciprocity.reciprocity_status <> 'left_only_asymmetric_source'
