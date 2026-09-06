/* Accumulated child windows must not duplicate a bill/subject membership. */
select bill_id, subject
from {{ ref('bills_subjects') }}
group by bill_id, subject
having count(*) <> 1
