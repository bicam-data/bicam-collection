{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'congresses'],
    indexes=[
      {'columns': ['congress_number'], 'unique': true},
      {'columns': ['start_year', 'end_year']}
    ]
) }}

select
    congress_number,
    congress_name as name,
    start_year,
    end_year,
    null::timestamptz as updated_at
from {{ ref('stg_congress__congress_detail') }}
where congress_number is not null
