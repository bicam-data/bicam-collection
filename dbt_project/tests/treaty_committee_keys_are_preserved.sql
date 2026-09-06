/* Treaty committee relationships are attached to the base treaty identity;
   repeated child observations must not duplicate the edge. */
select
    treaty_id,
    committee_id,
    count(*) as observed_rows
from {{ ref('stg_congress__treaty_committees') }}
where treaty_id = 'td118-13'
  and committee_id = 'HSAG'
group by treaty_id, committee_id
having count(*) <> 1
