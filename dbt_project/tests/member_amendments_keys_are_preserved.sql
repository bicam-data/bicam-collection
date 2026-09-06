select
    bioguide_id,
    amendment_id,
    sponsorship_type,
    count(*) as observed_rows
from {{ ref('stg_congress__member_amendments') }}
where bioguide_id = 'A000001'
group by bioguide_id, amendment_id, sponsorship_type
having count(*) <> 1
