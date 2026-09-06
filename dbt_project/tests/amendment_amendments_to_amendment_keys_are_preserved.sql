/* A repeated amendment-to-amendment child observation is one edge, not two. */
select
    parent_raw_id,
    lower(amendment_type) as amendment_type,
    amendment_number,
    congress,
    count(*) as observed_rows
from {{ ref('stg_congress__amendment_amendments_to_amendment') }}
where amendment_number = '2'
group by parent_raw_id, lower(amendment_type), amendment_number, congress
having count(*) <> 1
