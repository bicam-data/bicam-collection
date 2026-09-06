/* Amendment action retries collapse at the action identity while preserving
   the action-to-vote expansion handled by its separate key. */
select
    amendment_id,
    action_id,
    count(*) as observed_rows
from {{ ref('stg_congress__amendment_actions') }}
where amendment_id = 'samdt1-118'
group by amendment_id, action_id
having count(*) <> 1
