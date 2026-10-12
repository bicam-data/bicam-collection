# Main-11 source-boundary policy

This policy defines how the research release interprets GovInfo evidence beside
the authoritative Congress.gov records. It is deliberately separate from
ingestion completeness: a source boundary is not a missing crawl response.

## Authority and preservation

Congress.gov is authoritative for the eleven selected entity families:
amendments, bills, members, congresses, hearings, committees, committee
meetings, committee prints, committee reports, treaties, and nominations.
GovInfo BILLS, CRPT, SERIALSET, CPRT, and CHRG/JHRG are evidence sources used
where a counterpart is published. Every observed package, granule, source-only
record, and failed counterpart probe remains in raw/staging evidence with
provenance.
No source-boundary policy creates a synthetic authoritative record.

Cross-source marts expose two denominators:

* **Raw match rate** compares the full source union and therefore shows source
  coverage differences.
* **Eligible match rate** excludes only reviewed, evidence-backed source gaps
  (for example, a source's historical collection boundary or an expiring 404).
  It is the quality denominator for reconciliation.

Unreviewed missing records, contradictions, or malformed identities remain
quality exceptions and cannot be hidden by this policy.

## Congress.gov historical availability

The historical runner probes Congresses 1–119 for every partitioned family and
records empty early partitions as source evidence. The API's observed root
records begin at different Congresses: bills 6, amendments 97, committee
meetings 111, committee prints 94, committee reports 103, hearings 104,
nominations 97, and treaties 81. Members, committees, and Congresses are
unpartitioned. These lower bounds are measured from the materialized coverage
ledger and must not be converted into synthetic zero-row entities or treated as
unattempted partitions. If Congress.gov later exposes an earlier partition, the
scheduled refresh will ingest it and the lower-bound audit will change.

The current empty ranges are bills 1–5, 12, and 43–81; amendments 1–96;
committee meetings 1–110; committee prints 1–93 and 95–100; committee reports
1–102; hearings 1–103; nominations 1–96; and treaties 1–80 and 82–85. These
ranges are evidence-backed and remain subject to re-probe as the upstream
coverage table changes.

A current 2026-08-29 probe rechecked all 735 partitions in those ranges with
one-item list requests across 17 configured keys. Every response was HTTP 200
with zero items and no pagination count, confirming that none had become
available since the historical harvest. The complete probe result is retained
in `reports/historical-empty-partition-probe-20260829T171300Z.md`.

## GovInfo BILLS and Congress.gov bills

The current GovInfo BILLS corpus begins at Congress 103. Congress.gov bills in
Congresses 6–102 therefore remain authoritative Congress-only records and are
classified as a documented GovInfo coverage boundary, not failed joins. A
modern GovInfo-first bill whose Congress.gov counterpart is not yet visible is
probed directly; a confirmed 404 is retained as expiring source-gap evidence
and is periodically retried. A successful counterpart removes the gap on the
next model refresh.

Bill identity matching uses the exact normalized Congress, bill type, and bill
number from a Congress.gov text-version/package identity. Titles are
corroboration, never an identity substitute. GovInfo package versions remain
available through the bill-version bridge even when no Congress.gov text record
is present.

## CPRT and Congress.gov committee prints

Only GovInfo package details whose `collectionCode` is `CPRT` participate in the
committee-print reconciliation relation. GPO/GOVPUB leakage, unsupported
collection codes such as WPRT, malformed identifiers, and legacy volume/part
variants are retained in raw and the all-package evidence bridge, but are
excluded from the exact CPRT overlap denominator when they cannot represent a
committee print identity.

The current CPRT route has 6,959 CPRT package identities, plus 15 GOVPUB and
810 GPO leakage packages. The leakage remains queryable and counted separately
in the release manifest; it is never treated as a committee-print identity.

The exact identity is normalized from print type, jacket number, and Congress;
GovInfo's package order and optional volume/part suffixes are normalized before
comparison. CPRT exact overlap is informational until historical publication
coverage is independently established. The release must report Congress-only,
GovInfo-only, legacy-variant, malformed, and excluded counts rather than
collapsing them into one missing bucket.

## CRPT, SERIALSET, and Congress.gov committee reports

GovInfo package details whose `collectionCode` is `CRPT` participate in the
exact committee-report reconciliation relation. Three packages labeled
`GPO` have unambiguous `GPO-CRPT-*` package IDs and are included as CRPT
identities; the resulting CRPT identity population is 19,712 packages spanning
Congresses 104–119. `SERIALSET`
packages (142,992 identities spanning Congresses 15–103) are retained as a
separate historical serial-set evidence population; they are not CRPT report
identities and are not included in the exact CRPT overlap denominator. Three
other/non-report route IDs (including `GPO-J6-REPORT`) are retained as
raw/all-package evidence and excluded from the CRPT relation.

This split is a source-label policy, not a deletion: package and granule payloads,
identity fields, and provenance remain queryable in GovInfo staging. The release
must expose CRPT overlap and SERIALSET census separately so historical serial-set
coverage is visible without inflating committee-report match candidates.

## CHRG/JHRG and Congress.gov hearings/meetings

Congress.gov remains authoritative for hearing identity and coverage. GovInfo
CHRG/JHRG packages are matched only when their package identity and available
date/chamber evidence support the canonical hearing key. The current route has
45,838 CHRG identities; 966 GOVPUB and three GPO packages are retained as
leakage evidence and excluded from the exact hearing denominator. GovInfo-only
packages remain evidence records; a counterpart 404 is expiring source-gap
evidence, not a fabricated hearing.

Hearing-to-committee-meeting links are many-to-many and directional. The
release unions the Congress.gov hearing `associatedMeeting` direction, the
committee-meeting `hearingTranscript` direction, and evidence-backed GovInfo
links. A hearing may have no committee meeting, a meeting may reference several
hearings, and a joint event is represented with `chamber = 'joint'`; neither
side is duplicated to force a House/Senate pair.

## Operational review

The six-hour ingestion report publishes the current raw and eligible rates,
source-gap counts, and CPRT census. The hourly GovInfo window runner and the
six-hour Congress refresh re-run bounded counterpart probes and rebuild the
affected reconciliation graph. Policies may be tightened only with new source
evidence; changing a denominator to make a rate look better is not an
acceptable remediation.
