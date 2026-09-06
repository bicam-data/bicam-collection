/* A package-level PDF is valid public evidence even when GovInfo has not
   published a corresponding granule detail. A richer Congress.gov or granule
   rendition may win ranking, but the public report must retain some PDF. */
select distinct
    govinfo.canonical_report_id as report_id
from {{ ref('int_committee_reports__govinfo') }} as govinfo
join {{ ref('int_raw_responses__current') }} as raw
  on raw.raw_id = govinfo.source_raw_id
join {{ ref('committee_reports') }} as reports
  on reports.report_id = govinfo.canonical_report_id
left join {{ ref('committee_reports_texts') }} as texts
  on texts.report_id = govinfo.canonical_report_id
where govinfo.canonical_report_id is not null
  and nullif(raw.response #>> '{download,pdfLink}', '') is not null
  and nullif(texts.pdf, '') is null
