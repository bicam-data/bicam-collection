{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'members'],
    indexes=[
      {'columns': ['bioguide_id', 'role', 'congress', 'chamber'], 'unique': true},
      {'columns': ['congress', 'chamber']}
    ]
) }}

select distinct
    bioguide_id,
    role,
    congress,
    {{ normalize_chamber('chamber') }} as chamber,
    is_current
from {{ ref('stg_congress__member_leadershiproles') }}
where bioguide_id is not null
