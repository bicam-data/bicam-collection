/* The public/staging child grain is one current observation per bill/member.
   Repeated full/delta fetches remain available in the intermediate evidence
   relation and must not inflate the reconciled child relation. */
select
    bill_id,
    bioguide_id
from {{ ref('stg_congress__bill_cosponsors') }}
group by bill_id, bioguide_id
having count(*) > 1
