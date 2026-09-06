select treaty_id, treaty_part_id, committee_id
from {{ ref('stg_congress__treaty_committees') }}
where treaty_part_id <> treaty_id

union all

select treaty_id, treaty_part_id, committee_id
from {{ ref('stg_congress__treaty_committeeactivities') }}
where treaty_part_id <> treaty_id

union all

select
    treaty_id,
    treaty_part_id,
    committee_id
from {{ ref('stg_congress__treaty_committeeactivities') }}
group by treaty_id, treaty_part_id, committee_id, activity_name, activity_date
having count(*) > 1
