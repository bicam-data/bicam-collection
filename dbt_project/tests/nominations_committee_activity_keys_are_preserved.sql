/* Committee activity edges have an explicit public grain.  Repeated child
   observations must not create duplicate activity rows. */
select
    nomination_part_id,
    committee_id,
    activity_name,
    activity_date,
    count(*) as observed_rows
from {{ ref('stg_congress__nomination_committeeactivities') }}
where nomination_part_id = 'pn123-01-118'
  and committee_id = 'HSAG'
  and activity_name = 'Reported'
  and activity_date = date '2023-01-07'
group by nomination_part_id, committee_id, activity_name, activity_date
having count(*) <> 1
