/* A summary version is one public row per bill/version code. */
select bill_id, version_code
from {{ ref('bills_summaries') }}
group by bill_id, version_code
having count(*) <> 1
