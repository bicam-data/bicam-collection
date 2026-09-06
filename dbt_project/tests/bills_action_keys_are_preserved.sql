/* Bill actions are keyed by the source-normalized action identity. Retries
   and duplicate array entries must not create duplicate public actions. */
select
    bill_id,
    action_id,
    count(*) as observed_rows
from {{ ref('stg_congress__bill_actions') }}
where bill_id = 'hr1-118'
group by bill_id, action_id
having count(*) <> 1
