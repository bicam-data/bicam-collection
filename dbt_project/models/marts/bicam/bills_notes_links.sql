{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id', 'note_number', 'link_url'], 'unique': true}, {'columns': ['bill_id']}]) }}
with numbered as (
    select n.*, row_number() over (partition by n.bill_id order by n.note_id)::integer as note_number
    from {{ ref('stg_congress__bill_notes') }} n
    join {{ ref('bills') }} using (bill_id)
)
select distinct on (l.bill_id, n.note_number, l.url)
    l.bill_id, n.note_number, l.name as link_name, l.url as link_url,
    bills.updated_at
from {{ ref('stg_congress__bill_notes_links') }} l
join numbered n using (bill_id, note_id)
join {{ ref('bills') }} bills using (bill_id)
where l.url is not null
order by l.bill_id, n.note_number, l.url, l.name nulls last
