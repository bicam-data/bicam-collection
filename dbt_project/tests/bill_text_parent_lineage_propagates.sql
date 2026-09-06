/* Bill text-version rows retain the raw bill-detail parent used to resolve
   bill_id. A parent-only repair must therefore invalidate the incremental
   row even when the child payload version is unchanged. */
{{ config(tags=["operations", "coverage", "quality"]) }}

select versions.text_version_key
from {{ ref('int_congress_bill_text_versions') }} as versions
join {{ ref('int_raw_responses__current') }} as current_response
  on current_response.raw_id = versions.text_raw_id
where versions.bill_detail_raw_id is distinct from current_response.parent_id
