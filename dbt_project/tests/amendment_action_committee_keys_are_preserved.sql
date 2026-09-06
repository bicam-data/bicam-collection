/* Committee edges on amendment actions have their own natural pair key. */
select
    amendment_id,
    action_id,
    committee_code,
    count(*) as observed_rows
from {{ ref('stg_congress__amendment_actions_committees') }}
where amendment_id = 'samdt1-118'
group by amendment_id, action_id, committee_code
having count(*) <> 1
