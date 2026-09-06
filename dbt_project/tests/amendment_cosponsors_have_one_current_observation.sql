/* The staging projection must retain one current row per amendment/member. */
select amendment_id, bioguide_id
from {{ ref('stg_congress__amendment_cosponsors') }}
where amendment_id = 'samdt1-118'
group by amendment_id, bioguide_id
having count(*) <> 1
