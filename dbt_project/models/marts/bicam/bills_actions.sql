{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['action_id'], 'unique': true}, {'columns': ['bill_id', 'action_date']}]) }}
select distinct a.action_id::text as action_id, a.bill_id, a.action_code, a.action_date, a.text,
    a.action_type, a.source_system, a.source_system_code, a.calendar,
    a.calendar_number::integer as calendar_number
from {{ ref('stg_congress__bill_actions') }} a
join {{ ref('bills') }} b using (bill_id)
