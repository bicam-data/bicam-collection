select metric_id
from {{ ref('fct_committee_print_reconciliation_metrics') }}
where exact_match_count + congress_only_count <> congress_print_count
   or exact_match_count + govinfo_only_representative_count <> govinfo_representative_count
   or govinfo_representative_count + duplicate_variant_package_count <> govinfo_package_count
   or exact_match_rate < 0
   or exact_match_rate > 1
