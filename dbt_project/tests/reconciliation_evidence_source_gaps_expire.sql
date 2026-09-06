/* Automatically generated source-gap dispositions must be revisited.  A
   404 is evidence about one probe, not proof that a counterpart is absent
   forever. */
select
    exception_id,
    entity,
    canonical_id,
    reviewed_at,
    expires_at
from {{ ref('fct_reconciliation_exceptions') }}
where reviewer = 'system'
  and review_disposition = 'confirmed_source_gap'
  /* Existing, explicitly reviewed source-gap rules may intentionally use a
     longer policy expiry. Restrict this assertion to dispositions generated
     directly from the latest failed 404 probe. */
  and review_reason in (
      'Conventional counterpart package or detail probe returned HTTP 404.',
      'The expected Congress.gov hearing endpoint returned HTTP 404.'
  )
  and (
      expires_at is null
      or reviewed_at is null
      or expires_at <= reviewed_at
      or expires_at > reviewed_at + interval '31 days'
  )
