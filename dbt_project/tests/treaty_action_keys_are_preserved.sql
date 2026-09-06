/* Repeated treaty action observations must collapse at the source action
   identity before treaty action consumers use them. */
select
    treaty_part_id,
    action_id,
    count(*) as observed_rows
from {{ ref('stg_congress__treaty_actions') }}
where treaty_part_id = 'td118-13'
group by treaty_part_id, action_id
having count(*) <> 1
