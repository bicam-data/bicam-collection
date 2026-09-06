{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['action_id', 'committee_code'], 'unique': true}, {'columns': ['bill_id']}]) }}

with keyed as (
    select
        c.action_id::text as action_id,
        c.bill_id,
        c.committee_code,
        c.committee_code is not null as key_complete,
        row_number() over (
            partition by c.action_id::text, c.committee_code
            order by c.committee_name desc nulls last
        ) as key_rank
    from {{ ref('stg_congress__bill_actions_committees') }} c
    join {{ ref('bills_actions') }} a
      on a.action_id = c.action_id::text
     and a.bill_id = c.bill_id
)

select action_id, bill_id, committee_code
from keyed
where key_rank = 1 or not key_complete
