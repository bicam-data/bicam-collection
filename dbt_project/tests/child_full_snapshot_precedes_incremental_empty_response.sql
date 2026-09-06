/* A later empty incremental window must not replace a complete historical
   child snapshot, while a non-empty window must contribute newly changed
   rows.  These fixtures cover both cases in the current archive. */
with observed as (
    select bill_id, count(*) as cosponsor_count
    from {{ ref('stg_congress__bill_cosponsors') }}
    where bill_id in ('hconres69-108', 'hr1266-119')
    group by bill_id
), expected as (
    select bill_id, cosponsors_count
    from {{ ref('stg_congress__bill_detail') }}
    where bill_id in ('hconres69-108', 'hr1266-119')
)
select
    expected.bill_id,
    expected.cosponsors_count,
    observed.cosponsor_count
from expected
left join observed using (bill_id)
where expected.cosponsors_count <> observed.cosponsor_count
   or observed.cosponsor_count is null
