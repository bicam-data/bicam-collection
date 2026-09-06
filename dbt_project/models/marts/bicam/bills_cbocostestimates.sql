{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id', 'url'], 'unique': true}, {'columns': ['pub_date']}]) }}
select distinct on (c.bill_id, c.url)
    c.bill_id, c.description, c.pub_date::date as pub_date, c.title, c.url
from {{ ref('stg_congress__bill_cbo_cost_estimates') }} c
join {{ ref('bills') }} using (bill_id)
where c.url is not null
order by c.bill_id, c.url, c.pub_date desc nulls last, c.title, c.description
