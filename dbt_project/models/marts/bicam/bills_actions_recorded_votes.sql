{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['action_id', 'chamber', 'congress', 'session', 'roll_number'], 'unique': true}, {'columns': ['bill_id']}]) }}

with keyed as (
    select
        v.action_id::text as action_id,
        v.bill_id,
        {{ normalize_chamber('v.chamber') }} as chamber,
        v.congress,
        v.date::date as date,
        v.roll_number,
        v.session,
        v.url,
        row_number() over (
            partition by v.action_id::text, {{ normalize_chamber('v.chamber') }}, v.congress,
                         v.session, v.roll_number
            order by v.date desc nulls last, v.url desc nulls last
        ) as key_rank,
        v.chamber is not null
            and v.congress is not null
            and v.session is not null
            and v.roll_number is not null as key_complete
    from {{ ref('stg_congress__bill_actions_recordedvotes') }} v
    join {{ ref('bills_actions') }} a
      on a.action_id = v.action_id::text
     and a.bill_id = v.bill_id
)

select action_id, bill_id, {{ normalize_chamber('chamber') }} as chamber, congress, date, roll_number, session, url
from keyed
where key_rank = 1 or not key_complete
