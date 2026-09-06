-- Package C regression: repeated detail observations must not fan out the
-- public one-row-per-amendment relation.
select amendment_id
from {{ ref('amendments') }}
group by amendment_id
having count(*) > 1
