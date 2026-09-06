select relationship_type, left_id, right_id, count(*) as row_count
from {{ ref('fct_congress_internal_reciprocity') }}
group by 1, 2, 3
having count(*) > 1
