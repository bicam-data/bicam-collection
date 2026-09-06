/* Duplicate historical packages stay evidence-only and must not multiply
   reconciliation candidates. */
select candidates.candidate_id, candidates.govinfo_source_record_id
from {{ ref('int_committee_report_candidates') }} as candidates
join {{ ref('int_committee_reports__govinfo') }} as govinfo
  on govinfo.source_record_id = candidates.govinfo_source_record_id
where not govinfo.is_representative_package
