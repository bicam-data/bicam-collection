/* Repeated committee-side bill observations must collapse at the declared
   membership grain before the reciprocity bridge consumes them. */
select
    committee_id,
    bill_id,
    count(*) as observed_rows
from {{ ref('stg_congress__committee_bills') }}
where committee_id = 'HSAG' and bill_id = 'hr1-118'
group by committee_id, bill_id
having count(*) <> 1
