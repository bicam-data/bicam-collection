{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committee_reports'],
    indexes=[
      {'columns': ['report_id', 'treaty_id'], 'unique': true},
      {'columns': ['treaty_id']}
    ]
) }}

/* The relationship union retains treaty-side and report-side Congress.gov
   observations.  Normalize the union's report-set key to the public,
   part-aware report identity before exposing the edge. */
with report_sets as (
    select distinct
        lower(report_type) || report_number::text || '-' || congress::text as report_set_id,
        canonical_report_id as report_id
    from {{ ref('int_committee_reports__congress') }}
    where canonical_report_id is not null
), edges as (
    select distinct
        report_sets.report_id,
        regexp_replace(lower(relationships.left_id), '[a-z]+$', '') as treaty_id
    from {{ ref('int_congress_internal_relationship_union') }} as relationships
    join report_sets
      on report_sets.report_set_id = relationships.right_id
    join {{ ref('treaties') }} as treaties
      on treaties.treaty_id = regexp_replace(lower(relationships.left_id), '[a-z]+$', '')
    where relationships.relationship_type = 'treaty_committee_report'
      and relationships.left_id is not null
      and relationships.right_id is not null
)
select distinct report_id::text as report_id, treaty_id::text as treaty_id
from edges
where report_id is not null
  and treaty_id is not null
