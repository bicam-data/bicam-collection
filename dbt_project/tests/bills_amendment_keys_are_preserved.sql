/* Repeated observations of one bill amendment must collapse at the declared
   relationship grain before reciprocity consumes the staging relation. */
select
    bill_id,
    amendment_id,
    count(*) as observed_rows
from {{ ref('stg_congress__bill_amendments') }}
where bill_id = 'hr1-118'
group by bill_id, amendment_id
having count(*) <> 1
