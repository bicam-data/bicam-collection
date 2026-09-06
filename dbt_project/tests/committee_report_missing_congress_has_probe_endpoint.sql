/*
   Every GovInfo-only committee report must have a deterministic Congress.gov
   detail probe.  Without this endpoint the reconciliation audit can mistake
   the already-fetched GovInfo package for evidence that the missing Congress
   counterpart was attempted, leaving a source gap unowned by the queue.
*/
select canonical_id, congress_number, expected_missing_endpoint_url
from {{ ref('fct_reconciliation_exceptions') }}
where entity = 'committee_report'
  and missing_source = 'congress'
  and expected_missing_endpoint_url is null
