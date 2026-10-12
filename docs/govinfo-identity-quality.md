# GovInfo identity quality workflow

GovInfo committee-report metadata is retained as source evidence and cleaned
through explicit, reviewable layers:

- `committee_id_raw` preserves the original `authorityId`.
- `int_govinfo_committee_id_map` keeps exact Congress.gov committee codes and
  accepts an `o`→`0` correction only when the corrected value is an observed
  Congress.gov `systemCode`. Values that do not meet that evidence rule remain
  unmapped; valid codes such as `hsgo00` are never globally rewritten.
- `bioguide_id_raw` preserves the source value. IDs are normalized to uppercase
  and classified as valid, missing, invalid, or requiring review.
- Multi-legislator strings are flagged and also exposed through the
  evidence-only `int_govinfo_multi_legislator_candidates` model. The source
  member row remains intact; the model emits one row per named component and
  retains every Congress.gov candidate, but never applies an ambiguous name
  match automatically. Report-level Congress metadata takes precedence over a
  stale Congress value embedded in a nested member object.
- `govinfo_member_identity_overrides.csv` is the durable manual-review input.
  Reviewers should record the source key, committee/congress/state context,
  selected Bioguide ID, reviewer, date, and reason.
- `fct_govinfo_member_identity_exceptions` is the queue that exposes unresolved
  rows with committee, Congress, state, and raw-name context for cross-reference
  against Congress.gov. `fct_govinfo_data_quality_metrics` summarizes the
  unresolved and corrected populations.
- `fct_govinfo_member_identity_candidates` adds evidence-only Congress.gov
  candidates. Exact normalized-name matches are preferred; when GovInfo only
  supplies a surname, a weaker surname+Congress (and state when available)
  match is retained. A single candidate is still not applied automatically;
  the reviewer must confirm the committee context and then add a row to the
  override seed. Rows with no candidate remain explicit manual work, which is
  expected when GovInfo omits both a usable name and a Bioguide ID.
- `fct_govinfo_committee_id_exceptions` is the parallel queue for
  `committees[].authorityId` values that are not currently backed by an
  observed Congress.gov system code. Historical/terminated committees remain
  explicit review work rather than being silently dropped.
- `govinfo_committee_id_overrides.csv` records evidence-backed historical or
  typo corrections (source ID, canonical Congress.gov ID, reviewer, date,
  reason, and optional expiry). Ambiguous values such as `hoce00` remain in
  the exception queue instead of being forced to a committee.
- `int_govinfo_identity_fields` uses recursive JSONPath descent across GovInfo
  bill, committee-report, hearing, and committee-print package/granule
  payloads. It audits Bioguide, Thomas, and authority IDs in nested subfields
  as well as the expected `members[]`/`committees[]` arrays. Common casing variants
  (`bioGuideId`, `bioguideId`, `bioguideID`, snake-case, and corresponding
  Thomas/authority forms) are retained with the source key spelling; malformed
  values are exposed through `fct_govinfo_nested_identity_exceptions`. The
  committee-ID map deliberately consumes only `committees[].authorityId` from
  report/hearing/print package/granule payloads; other recursively discovered
  authority values stay in the general audit and are not misclassified as
  committee codes.

The current recursive audit covers 3,472,836 identity-bearing values across
all 14 package/granule list and detail node types in the harvested GovInfo
bills, committee reports, hearings, and committee prints. It found no invalid
Bioguide-shaped values; explicit Thomas-ID keys were not present in the current
payloads, while all authority/Thomas candidates remain retained for review.

The review loop is therefore: export the candidate/exception rows, verify the
member's Congress/state/chamber and committee service against Congress.gov,
then add the durable `source_member_key` mapping with reviewer/date/reason.
The raw GovInfo payload and the pre-resolution fields remain queryable after
the override is applied.

This keeps hand-researched decisions cumulative and auditable without turning
name similarity or a blanket character replacement into an untraceable merge.

The 2026-09-13 review also added two Congressional Directory overrides for
the 109th-Congress Washington 5 entry `CATHY McMORRIS`; Congress.gov terms
confirm Bioguide `M001159`. The remaining no-match rows in the current queue
are blank/vacancy directory entries, which are retained as explicit
non-person source evidence rather than assigned a fabricated member ID. Any
future named no-match remains a review item until the same Congress/state and
committee or directory evidence is recorded.
