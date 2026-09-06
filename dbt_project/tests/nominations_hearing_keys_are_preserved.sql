/* A nomination may repeat the same hearing across child polls; retain one
   source relationship at the nomination-part/hearing-part grain. */
select
    nomination_part_id,
    hearing_part_id,
    count(*) as observed_rows
from {{ ref('stg_congress__nomination_hearings') }}
where nomination_part_id = 'pn123-01-118'
  and hearing_part_id = 'hhrg10-1-118'
group by nomination_part_id, hearing_part_id
having count(*) <> 1
