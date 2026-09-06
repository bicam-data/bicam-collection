/* Repeated bill-side committee observations must collapse at the declared
   membership grain before the reciprocity bridge consumes them. */
select
    bill_id,
    committee_code,
    count(*) as observed_rows
from {{ ref('stg_congress__bill_committees') }}
where bill_id = 'hr1-118'
group by bill_id, committee_code
having count(*) <> 1
