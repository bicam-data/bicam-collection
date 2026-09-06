/* A committee-report text expansion depends on both the text payload and the
   detail payload that supplies report identity.  A detail-only version change
   must therefore invalidate the incremental row even when the text payload
   itself is unchanged. */
select formats.format_key
from {{ ref('int_congress_committee_report_text_formats') }} as formats
left join {{ ref('int_congress_committee_report_records') }} as reports
  on reports.source_raw_id = formats.report_detail_raw_id
where reports.source_raw_id is null
   or reports.source_raw_response_version_id is distinct from formats.report_source_raw_response_version_id
