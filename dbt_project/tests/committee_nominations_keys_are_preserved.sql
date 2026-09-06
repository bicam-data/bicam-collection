/* Committee nomination membership is keyed by its owning committee and
   nomination identity; retries and overlapping pages must not duplicate it. */
select
    committee_id,
    nomination_id,
    count(*) as observed_rows
from {{ ref('stg_congress__committee_nominations') }}
where committee_id = 'HSAG'
group by committee_id, nomination_id
having count(*) <> 1
