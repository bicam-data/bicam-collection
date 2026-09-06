#!/usr/bin/env bash
set -u

supervisor_pid=${1:?supervisor PID is required}
log_file=${2:-/tmp/bicam-post-crawl-audit.log}

while kill -0 "$supervisor_pid" 2>/dev/null; do
  sleep 60
done

{
  date -u
  echo "dbt_status"
  cat /tmp/bicam-dbt-safe-v4.status 2>/dev/null || true
  echo "unresolved_failures"
  docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -F $'\t' -c "
    with latest as (
      select distinct on (endpoint_url, node_type)
        endpoint_url, node_type, http_status, fetched_at
      from crawl_run_fetches
      where outcome = 'failed'
      order by endpoint_url, node_type, fetched_at desc
    )
    select http_status, count(*)
    from latest as failed
    where not exists (
      select 1 from raw_responses as responses
      where responses.endpoint_url = failed.endpoint_url
    )
    group by http_status
    order by http_status;
  "
  echo "table_counts"
  docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -F $'\t' -c "
    select 'fct_bill_matches', count(*) from fct_bill_matches
    union all select 'fct_committee_report_matches', count(*) from fct_committee_report_matches
    union all select 'fct_reconciliation_metrics', count(*) from fct_reconciliation_metrics
    union all select 'fct_collection_coverage', count(*) from fct_collection_coverage;
  "
  echo "metrics"
  docker exec bicam-collection-postgres psql -U admin -d bicam_collection -c "
    select entity, authoritative_source, candidate_count, accepted_count,
      ambiguous_count, unmatched_count, quality_status
    from fct_reconciliation_metrics
    order by entity, authoritative_source;
  "
} > "$log_file" 2>&1
