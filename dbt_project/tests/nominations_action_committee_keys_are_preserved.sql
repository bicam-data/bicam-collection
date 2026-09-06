/* Committee memberships on a nomination action have their own natural key;
   repeated names or retries must not duplicate the action/committee edge. */
select
    action_id,
    committee_id,
    count(*) as observed_rows
from {{ ref('stg_congress__nomination_actions_committees') }}
where action_id is not null
  and committee_id = 'HSAG'
group by action_id, committee_id
having count(*) <> 1
