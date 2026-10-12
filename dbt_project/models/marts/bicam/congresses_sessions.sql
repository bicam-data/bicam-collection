{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'congresses'],
    indexes=[
      {'columns': ['congress_number', 'session', 'chamber'], 'unique': true},
      {'columns': ['start_date']}
    ]
) }}

with normalized as (
    select
        congress_number,
        session,
        {{ normalize_chamber('chamber') }} as chamber,
        type,
        start_date,
        end_date
    from {{ ref('stg_congress__congress_sessions') }}
)
select distinct on (congress_number, session, chamber)
    congress_number,
    session,
    chamber,
    type,
    start_date,
    end_date
from normalized
where congress_number is not null
  and session is not null
  and chamber is not null
order by
    congress_number,
    session,
    chamber,
    (start_date is not null) desc,
    end_date desc nulls last,
    type nulls last
