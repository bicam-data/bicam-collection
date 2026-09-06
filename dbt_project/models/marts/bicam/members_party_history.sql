{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'members'],
    indexes=[
      {'columns': ['bioguide_id', 'party_code', 'start_year'], 'unique': true},
      {'columns': ['party_code']}
    ]
) }}

select distinct
    bioguide_id,
    party_code,
    party_name,
    start_year,
    end_year
from {{ ref('stg_congress__member_partyhistory') }}
where bioguide_id is not null
