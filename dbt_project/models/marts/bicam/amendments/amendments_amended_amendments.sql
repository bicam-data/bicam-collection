{{ config(
    schema='bicam', materialized='table', tags=['bicam_public', 'amendments'],
    indexes=[
      {'columns': ['amendment_id', 'amended_amendment_id'], 'unique': true},
      {'columns': ['amended_amendment_id']}
    ]
) }}

select distinct
    amendment_id,
    amended_amendment_id
from {{ ref('stg_congress__amendment_amended_amendments') }}
where amendment_id is not null and amended_amendment_id is not null

