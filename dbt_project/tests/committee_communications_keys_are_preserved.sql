/* A repeated committee communications observation is one source edge, not
   two public memberships.  The edge key includes its owning committee. */
with duplicate_keys as (
select
    committee_id,
    communication_id,
    count(*) as observed_rows
from {{ ref('stg_congress__committee_communications') }}
where committee_id = 'HSAG'
group by committee_id, communication_id
having count(*) <> 1
), stale_observation as (
select committee_id, communication_id, 1 as observed_rows
from {{ ref('stg_congress__committee_communications') }}
where committee_id = 'HSAG'
  and communication_id = 'house-ec3262-118'
  and communication_type_name <> 'Executive Communication (retry)'
)
select * from duplicate_keys
union all
select * from stale_observation
