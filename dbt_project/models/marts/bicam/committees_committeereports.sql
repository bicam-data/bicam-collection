{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committees', 'committee_reports'],
    indexes=[
      {'columns': ['committee_code', 'report_id'], 'unique': true},
      {'columns': ['report_id']}
    ]
) }}

select distinct
    left_id as committee_code,
    right_id as report_id
from {{ ref('int_congress_internal_relationship_union') }}
where relationship_type = 'committee_committee_report'
  and left_id is not null
  and right_id is not null
