/* Member-side legislation is a reciprocal edge and must not be inflated by
   repeated child-page observations. */
select
    bioguide_id,
    bill_id,
    sponsorship_type,
    count(*) as observed_rows
from {{ ref('stg_congress__member_bills') }}
where bioguide_id = 'A000001'
group by bioguide_id, bill_id, sponsorship_type
having count(*) <> 1
