{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id', 'note_number'], 'unique': true}]) }}
with numbered as (
    select n.*, row_number() over (partition by n.bill_id order by n.note_id)::integer as note_number
    from {{ ref('stg_congress__bill_notes') }} n
    join {{ ref('bills') }} using (bill_id)
)
select numbered.bill_id, note_number, note_text, bills.updated_at
from numbered
join {{ ref('bills') }} bills using (bill_id)
