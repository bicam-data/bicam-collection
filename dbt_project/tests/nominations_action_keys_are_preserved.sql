/* Repeated nomination action observations must collapse at the source action
   identity before the public one-row-per-action relation is built. */
select
    nomination_part_id,
    action_id,
    count(*) as observed_rows
from {{ ref('stg_congress__nomination_actions') }}
where nomination_part_id = 'pn123-01-118'
group by nomination_part_id, action_id
having count(*) <> 1
