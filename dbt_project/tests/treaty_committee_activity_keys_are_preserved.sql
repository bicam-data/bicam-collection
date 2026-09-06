/* Treaty committee activities use the same explicit public relationship
   grain as the nomination activity relation. */
select
    treaty_part_id,
    committee_id,
    activity_name,
    activity_date,
    count(*) as observed_rows
from {{ ref('stg_congress__treaty_committeeactivities') }}
where treaty_part_id = 'td118-13'
  and committee_id = 'HSAG'
  and activity_name = 'Considered'
  and activity_date = date '2023-01-09'
group by treaty_part_id, committee_id, activity_name, activity_date
having count(*) <> 1
