{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id', 'law_id'], 'unique': true}, {'columns': ['law_id']}]) }}
select distinct on (l.bill_id, l.law_number)
    l.bill_id,
    'PL' || l.law_number as law_id,
    l.law_number,
    case l.law_type
      when 'public' then 'Public Law'
      when 'private' then 'Private Law'
      else l.law_type
    end as law_type
from {{ ref('stg_congress__bill_laws') }} l
join {{ ref('bills') }} using (bill_id)
where l.law_number is not null
order by l.bill_id, l.law_number, (l.law_type = 'public') desc
