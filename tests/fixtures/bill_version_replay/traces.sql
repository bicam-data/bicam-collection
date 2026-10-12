-- Exact fixture build-input traces, not post-hoc production provenance.
select v.govinfo_package_id,
       to_jsonb(v) as public_row,
       to_jsonb(b) as bridge_input,
       to_jsonb(g) as bridge_source_version,
       to_jsonb(c) as congress_identity_source_version,
       to_jsonb(r) as rendition_source_version,
       to_jsonb(h) as html_extraction_input,
       raw.id is not null and b.govinfo_raw_response_version_id is distinct from raw.id as mixed_raw_versions,
       raw.id is null as unresolved_raw_version
from bicam.bills_versions v
join public.bridge_govinfo__bill_versions b on b.package_id = v.govinfo_package_id
left join public.input_raw_versions g on g.version_id = b.govinfo_raw_response_version_id and g.raw_id = b.govinfo_raw_id
left join public.input_raw_versions c on c.version_id = b.congress_raw_response_version_id
left join public.raw_response_versions raw on raw.id = b.govinfo_raw_response_version_id and raw.raw_response_id = b.govinfo_raw_id
left join public.input_raw_versions r on r.version_id = raw.id
left join public.html_text_successful_current h
  on h.url = nullif(raw.response #>> '{download,txtLink}', '') and h.extraction_status = 'success'
order by v.govinfo_package_id
