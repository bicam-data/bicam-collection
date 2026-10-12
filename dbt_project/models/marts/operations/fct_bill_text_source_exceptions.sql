{{ config(materialized='table', tags=['operations', 'quality', 'bills', 'identity_review']) }}

-- Reviewed exceptions survive refetches and do not rewrite immutable HTML or
-- infer replacement links. Future detected conflicts remain reviewable too.
select bill_id, source_url, response_hash, reason_code,
       verified_alternative_url, review_status, reviewed_at::timestamptz as observed_at
from {{ ref('bill_text_source_exceptions') }}
union all
select (html.error::jsonb)->>'expected_bill_id' as bill_id,
       html.url, html.response_hash, 'detected_identity_conflict',
       null::text, 'requires_review', html.fetched_at
from public.html_text_versions html
where html.extraction_status = 'identity_conflict'
  and not exists (
      select 1 from {{ ref('bill_text_source_exceptions') }} known
      where known.source_url = html.url and known.response_hash = html.response_hash
  )
