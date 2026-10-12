{{ config(
    schema='bicam', materialized='table', tags=['bicam_public', 'amendments'],
    indexes=[
      {'columns': ['amendment_id']},
      {'columns': ['action_id']},
      {'columns': ['action_id', 'chamber', 'congress', 'session', 'roll_number'], 'unique': true}
    ]
) }}

with keyed as (
    select
        action_id::text as action_id,
        amendment_id,
        {{ normalize_chamber('chamber') }} as chamber,
        congress,
        date::date as date,
        roll_number,
        session,
        url,
        row_number() over (
            partition by action_id::text, {{ normalize_chamber('chamber') }}, congress, session, roll_number
            order by date desc nulls last, url desc nulls last
        ) as key_rank,
        chamber is not null
            and congress is not null
            and session is not null
            and roll_number is not null as key_complete
    from {{ ref('stg_congress__amendment_actions_recordedvotes') }}
)

select action_id, amendment_id, {{ normalize_chamber('chamber') }} as chamber, congress, date, roll_number, session, url
from keyed
where key_rank = 1 or not key_complete
