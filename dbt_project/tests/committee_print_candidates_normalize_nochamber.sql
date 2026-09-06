select candidate_id, canonical_print_id, congress_print_id
from {{ ref('int_committee_print_candidates') }}
where canonical_print_id like 'nprt%'
   or congress_print_id like 'nprt%'
