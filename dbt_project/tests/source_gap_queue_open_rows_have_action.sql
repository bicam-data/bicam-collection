select queue_id, next_action
from {{ ref('fct_source_gap_queue') }}
where queue_status in ('open', 'due_for_reprobe')
  and next_action = 'no_action'
