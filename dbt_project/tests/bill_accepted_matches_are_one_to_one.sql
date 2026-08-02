select congress_source_record_id
from {{ ref('fct_bill_matches') }}
where decision = 'accepted'
  and congress_source_record_id is not null
group by congress_source_record_id
having count(*) > 1
