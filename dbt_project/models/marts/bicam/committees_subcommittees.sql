{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committees'],
    indexes=[
      {'columns': ['committee_code', 'subcommittee_code'], 'unique': true},
      {'columns': ['subcommittee_code']}
    ]
) }}

select distinct
    committee_id as committee_code,
    subcommittee_id as subcommittee_code
from {{ ref('stg_congress__committee_subcommittees') }}
where committee_id is not null
  and subcommittee_id is not null
