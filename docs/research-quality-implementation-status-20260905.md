# Research-quality implementation status

Latest Congress-119 timestamp/quality checkpoint (2026-09-11): candidate-8
retains 809,383 rows in 89 original core tables and adds 837 pre-snapshot
`updated_at` values that were present in retained Congress.gov raw payloads but
missing from staging (14 Congress rows and 823 committee-history rows). The
all-family Parquet package roundtrips through isolated PostgreSQL with all 110
FK definitions valid. A new 89-table quality audit now separates source-cohort
field gaps, semantic-domain signals, empty relationships and intentional raw-text
exclusion. The candidate semantic-pattern failure list is empty; historical-only
domain oddities are retained as observations. Remaining gaps are documented
rather than silently filled. The hash-bound [gap triage](../reports/quality/congress119-export-20260910/candidate-8-gap-triage.json)
binds all 29 reported fields to read-only staging evidence and refresh actions;
production is untouched. See [candidate-8](../reports/quality/congress119-export-20260910/candidate-8-README.md).

Latest Congress-119 directory checkpoint (2026-09-11): candidate-7 retains
809,383 rows in 89 original core tables and adds the remaining 33 safe directory
fields, for 1,614 total biography/population/official-URL cells across 538 terms.
All parseable member-office entries in the captured official directory now match
one 119th-Congress term; two source URLs are explicitly recorded as changed
aliases. The 17 members absent from this edition remain cohort exceptions because
the text was compiled October 1, 2025. Candidate-7 needs final PostgreSQL
roundtrip, coverage/domain audits and the two ICPSR policy decisions. Production
is untouched. See [candidate-7](../reports/quality/congress119-export-20260910/candidate-7-README.md).

Latest Congress-119 directory checkpoint (2026-09-10): candidate-6 retains
809,383 rows in 89 original core tables and applies 1,581 evidence-backed
`members_terms` fields across 527 rows (biography, 2020 Census population and
official URL). The other 88 files and inclusion ledger are byte-identical to
candidate-5. All 89 tables roundtrip through isolated PostgreSQL and all 110 FK
definitions validate. Eleven directory identities remain unresolved, the two
multiple-ID ICPSR cases remain pending, and broader source-coverage/domain
acceptance is still open. The official directory text is bound by SHA-256 and
its October 1, 2025 compilation cutoff is explicit. Production is untouched.
See [candidate-6 and remaining gates](../reports/quality/congress119-export-20260910/candidate-6-README.md).

Latest Congress-119 member checkpoint (2026-09-10): candidate-5 retains 809,383
rows in 89 original core tables and adds 553 evidence-backed ICPSR values. Only
those cells changed; 88 other Parquet files and the inclusion ledger are unchanged.
All rows roundtrip through isolated PostgreSQL and all 110 FK definitions validate.
65 focused tests and scoped Ruff pass. Kiley and Van Drew remain unresolved
pending the single-column multiple-ID policy. The retained 119th-Congress
directory granule list is empty, but its official 1.1 MB text has now been
captured as supplemental extraction evidence; no term fields have yet been
projected from it. Member-term restoration and broader feature/coverage acceptance
remain open. Production is untouched; the owned test container is stopped.
See [candidate-5 and next steps](../reports/quality/congress119-export-20260910/candidate-5-README.md).

Latest Congress-119 identity repair (2026-09-10): candidate-4 contains 809,383
rows in 89 original core tables. Jacket 60081 is now one `jhrg60081-119` root
with joint chamber; all declared hearing foreign keys and duplicate meeting,
committee/date links were reconciled together. Original package IDs, source
citations and rendition alternatives remain intact in a hash-bound sidecar.
All 809,383 typed rows roundtrip through isolated PostgreSQL, all 110 FK
definitions validate, and independent all-table delta validation has zero
unexpected changes. Sixty-six targeted tests and scoped Ruff pass; the bounded
test container is stopped and production is untouched. A captured Voteview-119
member-file audit supports 553 non-conflicting ICPSR enrichments; Kiley and
Van Drew require an explicit multiple-role-ID policy. Enrichment is not yet
applied. Member feature restoration and population-wide feature/coverage
acceptance remain open. See [candidate-4 and next steps](../reports/quality/congress119-export-20260910/candidate-4-README.md).

Latest Congress-119 hearing repair (2026-09-10): candidate-3 contains 809,388
rows in 89 original core tables. Restored 366 further hearing child rows from
28 exact pre-snapshot package/granule versions, including 318 member links;
one absent Bioguide ID resolves by unique exact full name/state/Congress/chamber.
Every typed row matches isolated PostgreSQL; all 110 FK definitions validate;
the exact all-table repair-delta audit has zero unexpected changes. Fifty-six
targeted tests and scoped Ruff pass. The test container is stopped; production
and prior exports are unchanged. Bounded GovInfo checks return 404 for four
expected package IDs, but 200 for the alternate House package for jacket 60081.
Its official text verifies a joint hearing; both existing House/Senate roots
also share one meeting. This is an explicit pending identity correction, not
just a source gap. Member feature parity and population-wide coverage/domain
checks remain open. See [candidate-3 and next work](../reports/quality/congress119-export-20260910/candidate-3-README.md).

Latest Congress-119 repair candidate (2026-09-10): candidate-2 contains 809,022
rows in all 89 original core tables. Applied 180 exact-identity amendment root
repairs and 956 child additions; all 7,038 amendments now have chamber/updateDate.
All 809,022 rows roundtrip through isolated PostgreSQL, all 110 FK definitions
validate, and an all-table predecessor comparison proves no changes beyond the
declared repairs. Fifty targeted tests and scoped Ruff pass. The 551 captured
action entries include 80 identical duplicates in HAMDT254; the established
JSONB action IDs correctly yield 471 distinct exported actions. SAMDT6656's
one/two cosponsor count difference is explained by a source-declared withdrawal.
The test container is stopped. Production and candidate-1 remain unchanged.
Still not release accepted: four hearing metadata gaps, two rendition gaps,
hearing child completeness, member feature restoration and broader source/domain
checks remain. See [candidate-2 evidence and remaining work](../reports/quality/congress119-export-20260910/candidate-2-README.md).

Congress-119 repair-evidence checkpoint (2026-09-10 17:33 UTC): all 180 targeted
amendment details passed exact-identity verification; all 387 advertised child
resources completed in 387 requests with no request errors. Preserved 551 actions,
85 cosponsor entries, 145 text-version metadata entries and one amendment-child
entry. One source count discrepancy remains (SAMDT6656 cosponsors: advertised
one, returned two). Also captured 14 hearing metadata and 14 metadata-granule
additions from retained pre-snapshot versions. These supplements are NOT yet
applied to candidate-1. Amendment observations postdate the frozen snapshot and
must be labelled as supplemental observations in the next candidate. Latest
targeted tests: 38 passed; scoped Ruff passed. Production remains unchanged.

Latest Congress-119 candidate checkpoint (2026-09-10): exported all 89 core
tables, 808,066 rows, with 20,798 semantic normalizations and fourteen
source-backed hearing-parent additions. All original headers match; all typed
values roundtrip through disposable PostgreSQL; all 110 declared foreign keys
validate. The package remains an incomplete candidate: 180 amendment detail
placeholders, hearing metadata gaps, member feature restoration and broader
semantic/source coverage checks remain. No production writes or promotion.
See [candidate, evidence, validation and remaining work](../reports/quality/congress119-export-20260910/README.md).

Active scope correction (2026-09-10, latest user instruction): export **Congress
119** across all eleven families and children, not records created/updated after
a date cutoff. The updated-since contract is superseded by
`bicam-collection/contracts/bicam_119_export.yaml`. Retain original schema,
semantic normalization and Parquet/Postgres validation, excluding document
raw-text bodies. Members are selected through 119th-Congress service; committees
need Congress-119 activity/service evidence; treaties qualify by receiving or
considering Congress. Required older references remain labelled dependencies.
Unknown timestamps no longer exclude records or block Congress selection;
incomplete metadata and the missing hearing parents still require repair.
The prior timestamp audit/evidence is preserved, not rerun as the active task.

Historical, superseded date-scope checkpoint (2026-09-10): that target selected
source-created/updated records since January 1, 2025, not introduced-since bills.
Completed hash/header/count scans of all 89 core tables (21,546,464 rows),
implemented export-only semantic/date rules, and recovered verified Congress
updateDate evidence for all 119 records from retained pre-snapshot raw versions.
314,788 bills qualify directly by update date; 716,951 chamber-value
normalizations are identified across the full inventory. The 14 hearing parent
gaps, 192 other unknown root dates, feature restoration, dependency closure and
final all-family export validation remain open. Production remains unchanged.
See [full-scope policy, evidence and next work](../reports/quality/updated-family-export-20260910/README.md).

Latest validation checkpoint (2026-09-09): repaired scheduler and retention
test doubles without changing operational behavior. The full suite passed
605 tests; all four opt-in database cases also passed separately. Two fresh
contract-4 raw-to-public replays produced identical hashes for all 91 public
relations and built the internal source-exception model. Three AI reviewers
compared 30 complete original HTML texts: all were source-relative faithful,
83 other cases lacked prose, and 113 linkage tasks were deliberately unreviewed.
Root corrected one conflicting rationale despite unanimous labels; original
votes are preserved. No human validation or population-accuracy claim is made.
Read-only live checks show EH1S now links to EH and the new version fields are
present; the internal exception registry still needs deployment authority.
The next daily export was not yet due; modern frozen-text review remains open.
See [results, evidence, limits and remaining deployment/review steps](../reports/quality/bill-text-review-20260909/README.md).

Latest remediation checkpoint (2026-09-08): implemented EH1S semantic/star-print
normalization, real-source fixtures, opening-heading conflict detection,
cached-text publication protection and a reviewed H.Res.15/109 exception.
Both frozen/local target inventories confirm EH1S was present: the gap was
matching, not missing ingestion. Bounded official catalog checks verify current
CPH/IH absences without rewriting historical labels; intended H.Res.15 IH XML
is verified. A newer September 8 packet contains 113 occurrences, including
30 with prose, but modern bill-text prose is still absent in that export.
102 focused tests passed with Postgres integration; the broader run exposed
seven separate scheduler-harness failures. Public contract source is version 4
with two additive version fields; no live rebuild or promotion was initiated.
See [implementation, evidence, limits and deployment handoff](../reports/quality/bill-source-remediation-20260908/README.md).

Latest review checkpoint (2026-09-08): three independent AI assessments
(including Luna) completed the 48-task stratified bill linkage/text packet.
46 tasks had unanimous labels; root adjudication retained two disagreements
as unresolved. Findings include one missed EH1S-to-EH semantic match, one
contradictory Congress.gov source, two faithful extracted texts and 22 absent
texts in the frozen September 6 candidate. All votes/evidence are preserved;
no human-independence attestation, accuracy estimate, live change or release
promotion was made. Review/aggregation validation passed 23 tests.
See [the consolidated results and next fixes](../reports/quality/ai-review-consensus-20260908/README.md).

Latest five-workstream checkpoint (2026-09-08 03:14 UTC): completed a bounded
read-only real-data comparison (10 versions / 28 links, no changes observed),
two fresh-database raw-to-public replays (91 relations, 39 populated, 53 rows,
identical file hashes), offline generation/row-trace verification, and review
packet preparation (48 stratified tasks plus 20 separate diagnostic tasks).
124 focused tests passed with integrations enabled; 24 follow-up checks also
passed. The complete fixture candidate validates but is NOT a production or
research-ready release. Independent review, wider lineage/coverage, and full
real-data acceptance remain open; no live promotion was performed.
See [the evidence, limits, and next handoff](../reports/research-quality-five-workstreams-20260908.md)
and [reviewer instructions](research-review-instructions-20260908.md).

Latest implementation checkpoint (2026-09-08): both bill-version defects from
the September 7 replay are fixed in source. Shared package/URL normalization
links `v1rh` to `RH`; rendition selection now joins the bridge's exact raw
version and owner, retaining unresolved packages with NULL renditions. Two
isolated builds produced identical hashes for six versions/twelve links;
22 SQL normalization cases and 93 focused tests (integration enabled) passed.
Full-project dbt parse passed. No live rebuild or promotion was performed.
See [the fixes, evidence, and deployment gate](../reports/research-quality-bill-version-fixes-20260908.md).

Latest frozen-replay checkpoint (2026-09-07 23:51 UTC): actual bill-version
publication SQL now runs twice against isolated synthetic boundary inputs,
with identical canonical/gzip hashes and fixture input traces for three
versions and ten links. A separate CI artifact job was added. This reproduced
a legacy `v1rh` URL-linking defect and a possible bridge/current-raw version
mismatch; neither production model was changed. See
[the replay evidence, limits, and next fixes](../reports/research-quality-bill-version-replay-20260907.md).
The final CLI replay passed; the focused suite passed 92 tests with one
explicitly guarded integration skip, and a separately enabled database test
run passed 14 tests. The disposable container is stopped. Upstream raw-to-public
recreation and independently adjudicated H/I evaluation remain open.

Latest source-evidence checkpoint (2026-09-07 23:29 UTC): portable bundle and
candidate validation now reject inconsistent source references/counts,
duplicate raw-version IDs, invalid observation metadata, symlinked evidence
directories, and undeclared nested inventory. Required evidence cannot be
empty or bind through absent snapshot timestamps. Endpoint/selection metadata
redaction was tightened; unchanged payloads still require credential review
before distribution. The focused suite passed 78 tests and an existing
three-payload disposable-database bundle passed offline verification.
See [the checkpoint and concrete remaining H/I work](../reports/research-quality-source-evidence-checkpoint-20260907.md).
This proves subset integrity, not public-row reproduction or a research-ready
release. No operational jobs, database data, or release pointers were changed.

Latest research-quality correction (2026-09-07): the offline evaluator now
counts actual text separately from rendition URLs and preserves PostgreSQL CSV
NULL/empty/literal-marker distinctions. Its full scan covered 91 relations and
22,663,245 rows in 146.57 seconds at 85,108 KiB peak RSS, verifying artifact
hashes, headers, and row counts. Twelve targeted regression tests passed.
See [the correction and evidence](../reports/research-quality-evaluator-correction-20260907.md)
and [the full report](../reports/quality/offline-candidate-evaluation-20260907-full.json).
This supersedes the September 6 evaluator's inflated text counts and claimed
NULL ambiguity. The same candidate contains 45,874 bill-text and 50,749
bill-version rows with content. Later live extraction is continuing separately.
H/I adjudication, row-to-source traces, and frozen-input recreation remain open.

Integration checkpoint: 2026-09-06. The 2026-09-04 agent plan is now the
acceptance contract for the active goal; the goal remains active until a
candidate satisfies the required release milestone.

This status integrates the 2026-09-04 agent implementation plan into the
active BICAM goal. The plan supersedes older notes that described the product
as fully validated. The corrected-release milestone is not complete while any
required package or the large HTML text backlog remains unfinished.

## Scope and release milestones

The public product remains the eleven BICAM root families and their child
relations, with the `bicam` PostgreSQL schema and plural relation names. The
core release stays provenance-light; source evidence, raw versions, and
version-level bill supplements remain available in supplemental artifacts.

- **Corrected release (A-G):** correct child state and public grains, execute
  the contract against rows and files, and validate a candidate before
  promotion.
- **Research-ready release (H-I):** complete portable provenance/version
  evidence and add evidence-based coverage, linkage, text, and researcher
  documentation.

Neither milestone is declared complete by a passing operational checkpoint.

Latest implementation checkpoint: [09-04 plan integration checkpoint](../reports/research-quality-plan-integration-checkpoint-20260905T220100Z.md).

Integration verification at 2026-09-06 08:05 UTC passed shell syntax checks for
the GovInfo, Congress supplemental-refresh, and HTML checkpoint wrappers plus
their focused regression slice (`10 passed`). This verifies the operational
bindings added by the 09-04 plan; it does not certify a live refresh or release.

Package H now has a bounded source-evidence exporter at
`bicam-collection/scripts/export_source_evidence.py`. It writes immutable raw
payloads by content hash, sanitized endpoint identities, raw-version IDs, and
observation metadata with a manifest checksum. A disposable PostgreSQL run
exported and independently verified three Congress bill observations in
`reports/source-evidence-ci-20260906T0820Z/`; this is representative evidence,
not yet the complete release bundle. The companion
`bicam-collection/scripts/verify_source_evidence.py` now verifies the bundle
offline and rejects payload tampering.

The dbt profile now maps `PGAPPNAME` into PostgreSQL's connection
`application_name`; the live GovInfo reconciliation retry confirmed the
unique per-run name, making fail-closed orphan cancellation selective instead
of dependent on the generic `dbt` label.

The 08:37 UTC GovInfo checkpoint completed all four bounded windows and both
identity checks, but the targeted reconciliation graph hit its 900-second
bound (`status=124`). Its unique backend was cancelled after the client exited;
the checkpoint is non-certifying and is recorded at
`reports/govinfo-window-checkpoint-20260906T083729Z.md`. The HTML lane then
reacquired the lock and resumed bill-text fetching.

The HTML checkpoint wrapper now persists `dbt_batches_since_last` and defers
the expensive resolver-backed propagation until six successful rotations by
default (`BICAM_HTML_DBT_BATCH_INTERVAL`, or `BICAM_HTML_DBT_FORCE=1` for an
immediate run). This keeps raw extraction moving while preserving a durable
pending flag and fail-closed propagation retries. It also flushes at a complete
source-table rotation, so a short final queue cannot leave pending text
indefinitely.

Latest local verification after the Package-D scalar foreign-key, wrapper, HTML-source coverage,
and candidate source-bundle, bill-relationship, treaty-action, treaty-committee, committee-edge, nominee-lineage, remaining-child, source-gap-report, adversarial-fixture, immutable-version, candidate-comparison, source-gap-probe, and HTML-propagation guards is `422 passed, 5 skipped`; the guarded skips are storage integration
tests when no disposable test DSN is supplied.

At 2026-09-06 06:11 UTC, the scheduled GovInfo reconciliation lane exposed a
real dependency-selection defect: its 34/35-model build failed closed because
`fct_source_gap_queue` was selected without its required
`fct_child_state_gaps` ancestor. The GovInfo window, identity audits, and
targeted reconciliation models completed, but that checkpoint is
non-certifying (`status=1`). All three scheduled refresh wrappers now select
`+fct_source_gap_queue`; an isolated disposable run rebuilt the full 61-model
ancestor closure and passed. The next scheduled operational refresh must
materialize this corrected queue before its live status can be accepted.

The same read-only audit showed why the committee-report exception count was
not merely a missing-probe problem: 604 of the 611 open Congress-side report
exceptions already had bill-side `committeeReports` evidence. Congress report
reconciliation now unions standalone report-detail records with bill-side
report identities, prefers the descriptive standalone record when both exist,
and uses bill-side evidence as the fallback candidate when it does not. The
expanded fixture graph materialized 66 models; the union assertion and
reconciliation metrics passed, with both bill/report evidence directions
preserved. The next operational refresh must materialize this model before
those 611 exceptions can be re-evaluated.

At 06:30 UTC the serialized operational refresh had already materialized the
corrected Congress report evidence: `fct_committee_report_matches` showed
20,204 accepted candidates versus 19,597 previously. The downstream exception
table was still on its prior generation at that checkpoint, so the 611 open
rows remain non-authoritative until the refresh finishes.

The public `committee_reports` mart now left-joins descriptive report details
and assigns bill-side-only Congress identities a lower source priority than a
standalone Congress detail or GovInfo record. This keeps a bill-side-only
report in the public union without replacing a richer description with nulls;
its public-union regression passed in the disposable graph.

The 06:11 UTC supplemental refresh reached the 3.27M-row child-state
selection rebuild but timed out before downstream queue/metrics materialization;
its fail-closed result remains non-certifying (`dbt=124`, open committee-report
exceptions still 611). The explicit timeout artifact is
[`congress-supplemental-timeout-20260906T0656Z.md`](../reports/quality/congress-supplemental-timeout-20260906T0656Z.md).
Scheduled dbt wrappers now pass a PostgreSQL statement timeout inside their
client timeout, and the HTML wrapper records propagation failures while
retaining its durable cursor and `pending_dbt` flag. The 07:03 UTC GovInfo
congressional-directory refresh passed (239 packages, 15,024 granules, 12,887
member-class granules); the interrupted concurrent GovInfo attempt is recorded
in [`govinfo-window-interrupted-20260906T0730Z.md`](../reports/quality/govinfo-window-interrupted-20260906T0730Z.md).
The 07:44 UTC HTML checkpoint added 500 bill-text, 500 bill-version, and 4
hearing successes; its propagation retry remains queued after a bounded dbt
timeout. No release pointer was promoted.

Current disposable-Postgres follow-up verification (2026-09-06 04:24 UTC)
passed all six Congress.gov reciprocity/union assertions and all seven
supplemental bill-version/linkage assertions (`PASS=6/6` and `PASS=7/7`).
These checks confirm that the 09-04 plan's reciprocity audit and version
surface are wired into the candidate graph; they do not certify a production
release or close the remaining A--G/H--I gates.

At 2026-09-06 05:51 UTC, the two new fail-closed guards also passed against
the disposable graph (`PASS=2/2`): missing-Congress committee reports now have
an explicit Congress.gov probe endpoint, and a later complete empty child
generation retires the earlier non-empty bill-subject observation.

The bounded title-selection workload now runs in CI and uses temporary tables
only: 100,000 bills with three observations each (300,000 title rows), a
5,000 ms isolated-Postgres execution threshold, and an observed 786 ms run at
2026-09-06 06:00 UTC. The plan has one window sort and spilled 52,888 KiB;
this is retained as performance evidence rather than hidden behind a pass
flag. The JSON artifact is
`reports/quality/title-selection-benchmark-20260906T0600Z.json`.

The next Package-G hardening step added an independent
`congress_internal_reciprocity_metrics_are_fail_closed` dbt assertion. It
rejects any future metric row that reports `pass` for a zero-edge,
endpoint-unavailable, asymmetric, contradictory, or unclassified family.
The three reciprocity metric assertions pass in the disposable database
(`PASS=3/3`). The fixture corpus now also has an explicit adversarial coverage
guard for duplicate-in-payload versus repeated-fetch observations, same-
position nominee negative controls, distinct bill IDs, two roll calls on one
action, and baseline-plus-delta report text; the full local suite is now
`422 passed, 5 skipped` with Ruff and `git diff --check` clean.

The following Package-B correction moved the reusable child transport scan
from current URL rows to immutable `int_raw_response_versions`, retaining the
current fetch timestamp when available and falling back to version
first/last-seen timestamps for historical payloads. Selection identity is now
`raw_response_version_id`, while the source `raw_id` remains available for
lineage; the complete child-state graph rebuilt `37/37` models and the
source-gap/reconciliation graph rebuilt `61/61` models. The focused child
selection/gap assertions passed `6/6`, including the immutable-version
lineage check. This closes the prior URL-upsert blind
spot. The full 89-relation candidate comparison now passes. The expanded
fixture graph also passes the explicit source-removal slice: a later complete
empty child response retires an earlier non-empty generation under normalized
transport identity, while the public observation relation is empty. The
remaining B gate is production-scale candidate binding; the title performance workload now
passes its bounded 5,000 ms threshold.

In the disposable corpus, 65 immutable Congress payload versions now produce
40 transport facts and 27 selected child pages; several endpoint raw IDs have
multiple historical versions, so this correction is non-vacuous rather than
only an interface change.

The first candidate-comparison implementation required by the 09-04 plan is
now available as `scripts/compare_release_candidates.py`. It accepts two exact
timestamped `manifest.json` paths (never `latest`), rejects symlinked
candidates/artifacts, hashes and parses the declared gzip CSVs, and uses a
temporary SQLite index so large relations are compared without retaining all
rows in memory. Reports include relation inventory, manifest/header/hash
status, added/removed/retained rows, changed keys, duplicate-key examples,
bounded row previews, and per-Congress row/key deltas. CI now exercises a
self-comparison of the complete disposable candidate. The full historical
comparison of the 2026-09-04 and 2026-09-05 candidates covered all 89
relations, including the large text artifacts; its exact inventory, row
counts, ordered headers, and SHA-256 hashes all pass. It confirms 256,540
nominee rows removed alongside the duplicate-key reduction, preserves 1,876
committee-print text rows, and records title/action/amendment/report deltas in
`reports/release-candidate-comparison-full-20260906T0455Z.json` (with a
human-readable companion `.md`). The source-removal behavior and bounded title
performance workload are now covered; remaining B/G evidence is the
candidate-bound corrected export.

The expanded disposable candidate is
`reports/exports/ci-20260906T0555Z/20260906T055410Z/manifest.json`; its exact
91-relation self-comparison is
`reports/exports/ci-20260906T0555Z/self-comparison.json`. Dry-run validation,
isolated promotion, and copied/hash-drift rejection all passed without
touching any production `latest` pointer.

The source-gap audit also exposed an operational blind spot: 611 GovInfo-only
committee-report exceptions were marked open, but their queue rows inherited
the already-fetched GovInfo URL and therefore could not prove that the
Congress.gov counterpart had been probed. The reconciliation exception model
now constructs the canonical Congress.gov detail endpoint from the report
type, number, part, and Congress (`/committee-report/{congress}/{type}/{number}`)
for every missing-Congress committee report. The managed queue can therefore
render these as `fetch_missing_source` work until a targeted probe returns a
200 or a confirmed 404; no source gap is auto-classified as resolved. The new
dbt assertion `committee_report_missing_congress_has_probe_endpoint` and the
disposable graph pass. The next serialized operational refresh must materialize
this change before the live freshness gate can reflect it.

## Integration decision recorded 2026-09-06

The 2026-09-04 agent implementation plan is adopted as the binding acceptance
contract for the active goal. This is an integration of execution and release
criteria, not a change to the product scope:

- The existing goal still covers incremental propagation, Congress.gov
  reciprocity, the supplemental bill-version surface, the managed source-gap
  queue with alerts disabled, and linked HTML extraction with PDF/XML out of
  scope.
- Packages A–G are the corrected-release gates; packages H–I are the separate
  research-ready gates. A live refresh, green quality command, or successful
  model build is evidence for a workstream but cannot by itself complete the
  goal or move a release pointer.
- All work must remain candidate-bound, reproducible, and fail-closed, using
  the plan's interfaces for child-state evidence, honest public grains,
  executable row/file validation, build/frontier binding, and atomic
  promotion. The eleven plural BICAM root families and the core public
  surface remain unchanged.
- The current completion blockers are explicit: the full adversarial fixture
  matrix and final A–G candidate comparison/promotion evidence, then H–I
  provenance/recreation and research-limitations evidence. The bounded HTML
  backlog and the queued operational corrections remain active work, not
  release completion evidence.

This section is the goal-level handoff for subsequent agents; the dated plan
retains the detailed package requirements and acceptance matrix.

### Package-B follow-up, 2026-09-06

The reusable child-selection materialization is now implemented as the paired
`int_congress_child_response_selection` (transport facts) and
`int_congress_child_response_selections` (durable selected pages) relations. It
performs the expensive Congress.gov JSON-shape, logical-collection,
pagination, and array-shape scan once per refresh; `resolve_child` remains the
source-compatible filtered interface. A disposable `+fct_child_state_gaps`
run rebuilt 36 models successfully, and the child-window, non-paginated-array,
and actionable-gap assertions passed 3/3. The relations are indexed by raw ID,
collection, and logical endpoint for bounded downstream scans.
The shared scan is explicitly limited to the registered child node types,
including the three paused House families; root/detail payloads are not
recursively walked unless a future child caller adds them to that registry.

The four remaining incremental child expansions (bill text versions/formats
and committee-report records/text formats) now rebuild as tables. This is
intentional: an authoritative empty complete child response must retire prior
rows; an incremental `delete+insert` expansion cannot delete keys that are
absent from the new source result. The raw current/version relations remain
incremental because they are append/current-state storage layers, not public
child expansions. The source-removal guard is covered by the incremental
propagation unit suite and the expanded 42-payload candidate matrix.

The source-gap reporter was exercised against the disposable database and
wrote a two-row report. Its child-state entries are rendered as actionable
`reprobe` items; date-window/page variants collapse to one logical collection
before entering the queue. The report path remains safe by default and does
not enable Pushover without explicit credentials and a CLI switch.

The HTML checkpoint wrapper now selects each text leaf with its resolver-backed
ancestors (`+bills_texts`, `+committee_prints_texts`, and the other in-scope
text models). This closes a propagation hole where newly harvested raw HTML
could remain invisible when only the public leaf model was rebuilt. The next
serialized operational checkpoint will therefore repopulate the currently
empty `bicam.committee_prints_texts` table from its already-fetched 1,876 raw
child pages before the extractor reports targets for that family.

The disposable verification of the exact selector rebuilt 10 ancestors/models
and produced one populated `bicam.committee_prints_texts` row with its HTML
URL. Its progress artifact is
`reports/html-text/disposable-20260906T0607Z/progress.md`, confirming that the
ancestor-selection fix changes the public target surface rather than merely
changing shell arguments.

`fct_child_state_gaps` now reads the durable selected-page relation in addition
to unnested observations, so an incomplete payload with an empty array still
creates a queue item instead of vanishing because there is no child row to
expand.

## How the plan is integrated into the active goal

The original active goal remains the source of product scope. The September 4
plan adds release-quality gates around each deliverable; it does not replace or
narrow them:

The inherited objective is to make incremental propagation correct, make
Congress.gov reciprocity auditable, expose the supplemental bill-version
surface, operate the source-gap queue with optional (disabled) alerts, and
extract available HTML text while leaving PDF/XML parsing out of scope. The
plan adds the conditions under which those deliverables may be called
production-quality: isolated evidence, honest public grains, executable
contract checks, candidate-bound build/frontier evidence, fail-closed
promotion, CI regression coverage, and research-ready provenance and
limitations documentation.

This is now the implementation contract for the active goal, rather than a
historical recommendation. Work may continue in the acquisition and HTML
lanes, but a workstream is not considered finished until its output is bound
to the candidate-level checks below. In particular, a live database quality
gate, a successful scheduled refresh, or a passing model build cannot by
itself move the goal to “complete.” The completion decision is made against a
single isolated candidate and the A–I acceptance matrix in the 09-04 plan.

The current execution order is deliberately dependency-aware:

1. Bind the corrected production-scale export to one completed build, its
   metrics/frontiers, and the candidate comparison; the expanded disposable
   candidate lifecycle is already passing.
2. Run the next serialized operational refresh to materialize the committee-
   report probe correction, reevaluate the 611 open GovInfo-only exceptions,
   and validate the corrected amendment-vote key.
3. Complete H–I as the research-ready layer: source-evidence and dependency
   capture beyond the new provenance sidecar, frozen-input recreation,
   coverage/linkage/text evaluation, data dictionary, and loading examples.

The latest operational evidence remains serialized: HTML extraction published
another successful 250/250 bill-text and bill-version rotation at 02:07 UTC;
fetch-lineage completed its 02:10 UTC freshness pass; GovInfo then acquired the
lock for its identity/reconciliation refresh. No job was manually restarted.

Since then, GovInfo completed the 02:05 UTC window at 02:17 UTC with zero
failed events and all targeted checks passing. HTML completed another 250/250
rotation at 02:25 UTC, with one hearing retry recovering successfully; the
latest extraction artifact is `reports/html-text/progress.md`.

This ordering keeps the original active deliverables moving in parallel where
safe, but prevents a partial or live-only result from being presented as the
polished research release.

### Interface decisions carried forward from the plan

- **B:** child selection output keeps endpoint/logical collection, page and
  offset, raw-version/fetch evidence, full-versus-delta classification, and
  completeness status; downstream models must not receive only a payload.
- **C:** contracts distinguish a true unique key, a grouping key, export
  ordering, required values, and foreign keys. A relation without a defensible
  public unique key is documented as such rather than assigned a positional
  surrogate.
- **D/E:** validation returns structured pass/fail results with a contract or
  bundle identity, machine-readable check IDs, counts, and bounded examples;
  errors and timeouts fail closed rather than becoming an empty success.
- **E/F/H:** release manifests carry an explicit schema/profile version, build
  ID, contract digest, relation inventory, source frontiers, and hashed
  references to immutable evidence without putting circular content hashes in
  the content being hashed.

| Active-goal deliverable | Current implementation surface | 09-04 gates now governing completion |
| --- | --- | --- |
| Incremental propagation | Immutable raw versions, `resolve_child`, materialized child observations, lineage tests | A/B: isolated fixtures, complete-baseline evidence, delta/removal semantics, and no mixed generations |
| Congress.gov reciprocity audit | Directional internal-edge union, reciprocity metrics, and union bridges | B/D/G: row-level cardinality and source-evidence checks in an exact candidate, including expected hearing/meeting asymmetry |
| Supplemental bill-version surface | `bicam.bills_versions` and `bicam.bills_version_links`; version history is supplemental and does not depend on bill child-endpoint completion | C/H: honest version evidence, source frontiers, reproducible release binding, and explicit supplement inventory |
| Managed source-gap queue with optional alerts | `fct_source_gap_queue`, scheduled reports that recompute expiry-sensitive status at read time, and disabled-by-default Pushover adapter | D/F/E: bounded reviewed gaps, expiry/reprobe evidence, candidate-bound metrics, and fail-closed promotion; credentials must be supplied before alerts can be enabled |
| HTML text extraction | Append-only `html_text_versions`, latest-success projections, bounded scheduled extractor, and progress artifacts | A/F/H/I: source/effective-frontier evidence, reproducible text artifacts, coverage denominators, and manual extraction evaluation; PDF/XML remain out of scope |

Accordingly, an operational green checkpoint is evidence for a workstream,
not evidence that the research-quality release is complete. The corrected
product cannot be promoted until A–G pass against one exact candidate; the
research-ready release additionally requires H–I. The acquisition and HTML
lanes may continue while those release gates are implemented.

## Package status

| Package | Current state | Evidence or remaining gate |
| --- | --- | --- |
| A — isolated tests/fixtures | In progress | Storage integration tests now require `BICAM_TEST_DATABASE_URL` and never fall back to `.env`/`DATABASE_URL`; guard tests cover that precedence. The disposable CI database run now passes all 7 storage lifecycle/version-lineage tests; the adversarial fixture matrix still needs to execute in CI. |
| B — child-state reconstruction | In progress | `resolve_child` now retains the newest demonstrable complete baseline plus every applicable non-empty delta, ignores empty incremental polls, allows a later valid empty array to retire prior membership, and does not treat an array-less/error-shaped response as a replacement. It now proves complete multi-page generations from crawl-run, API-total, array-count, and contiguous-offset evidence; incomplete histories retain the old page-level fallback with an explicit completeness flag. A no-pagination child payload is now treated as a complete single-payload generation (including an explicitly empty full array), while empty deltas remain non-destructive; logical collections normalize known transport parameters without discarding unknown identity parameters. Bill cosponsor, amendment cosponsor, nominee, bill title, bill subject/summary, bill action, amendment action, committee-report membership, bill-text-version, committee-report-text, committee-print-text, amendment-text-version/format, member-legislation, related-bill, bill-amendment, bill-committee, treaty, committee, nomination, and amendment-relationship observations now retain resolver-selected raw-version/crawl-run/generation evidence. All in-scope Congress.gov staging callers consume observation relations rather than independently expanding child state; only the three paused House requirement/vote families retain direct calls. The caller inventory is recorded in [child-state-caller-inventory-20260905.md](child-state-caller-inventory-20260905.md). The reusable selection table now centralizes the JSON-shape scan, and the four incremental child expansions rebuild as tables so complete replacements can retire rows. The full 89-relation candidate comparison now supplies before/after evidence; the source-removal fixture slice and bounded title workload pass, leaving candidate binding as the remaining B gate. |
| C — public grains | In progress | Nominee repeated observations are reconciled through `int_congress_nominee_observations`; public nominee duplicate tests pass. Contract version 3 now declares `nominations_nominees`, `bills_texts`, and `amendments_texts` as grouping-key relations where the published columns cannot guarantee a unique rendition key; source-null dates are no longer falsely required. Bill titles now choose one deterministic metadata row per `(bill_id, title_type, title)`, and recorded-vote contracts use action/chamber/Congress/session/roll identity. The bill and amendment vote marts now enforce that five-column key deterministically and preserve every source key; the bill-action committee edge enforces `(action_id, committee_code)`, and related-bill edges enforce `(bill_id, related_bill_id)`. A non-empty disposable corpus proves rolls 183/184 and 354/355, a duplicate committee-code observation, and a duplicate related-bill observation are reduced to their declared public edges. The reciprocal HSAG↔hr1-118 fixture now reaches the canonical internal reciprocity fact with both directions and `reciprocity_status=reciprocal`. The `amendments` mart now ranks repeated detail observations before joining and has an explicit one-row-per-`amendment_id` regression test. The live 91-relation row audit passes these corrected semantics. A read-only archive-parity audit now confirms exact 11-family/90-archive-relation coverage and 89/89 current core relation presence; its JSON and Markdown artifacts are `reports/public-archive-parity-20260906.json` and `.md`. The bounded historical candidate comparison now provides before/after evidence for nominee, title, vote/action, amendment, report, and committee-print changes; the full relation comparison and explicit retention/legitimacy classifications remain. Count differences are retained as evidence rather than treated as automatic errors; the only empty current core relation is the known pending `committee_prints_texts` refresh. |
| D — executable contract validation | In progress | `validate_bicam_product.py --check-data` performs bounded row-level unique-key, required-value, scalar foreign-key, and declared composite-foreign-key checks with machine-readable check IDs and examples; the current supplemental database and disposable candidate pass all 506 checks. Composite references use an additive `foreign_key_constraints` contract form and fail closed when malformed or unresolved; scalar references now also fail closed when either declared column is absent. Artifact: `reports/public-contract-data-validation-20260905T171221Z.json`. The exporter now runs the same row checks inside its repeatable-read transaction and embeds the result in candidate build evidence. Root/child coverage evidence is now filtered to the authoritative main-11 Congress scope, so paused categories cannot invalidate a main-11 candidate. |
| E — candidate validation/promotion | In progress | `export_research_snapshot.py` creates an immutable candidate without touching `latest`; `promote_research_snapshot.py` validates the exact manifest, portable `provenance.json` sidecar, source-bundle identity for supplemental profiles, contract inventory, headers, row counts, hashes, supplemental scope, and candidate-bound build evidence before a serialized atomic symlink swap. Focused tests cover dry-run preservation, malformed headers, hash drift, source-bundle drift, partial inventories, sidecar digest drift, and symlinked candidates. CI now runs the complete export/validate/promote path in a temporary directory. The corrected full-database candidate also passed exact dry-run and temporary promotion, while an altered manifest failed closed and preserved the accepted pointer. Deterministic exports use gzip level 6 to avoid wasting CPU on the large HTML/text relations; production promotion remains intentionally separate. |
| F — build/source-frontier evidence | In progress | The exporter now records a build ID, contract/pipeline digests, relation counts, source frontiers, quality-metric summaries, source-gap counts, same-snapshot row-validation results, and a portable `provenance.json` sidecar bound to the manifest digest. It now also records an explicit source-bundle identity: allowlisted source roots, Git revision/worktree state, source-tree digest, `uv.lock` digest, local Rotisserie revision/tree digest, and runtime metadata. The corrected full-database candidate bound 91 tables to a build, 32 source frontiers, and 506/506 passing row checks; full frozen-input recreation and generation IDs remain. |
| G — CI integration | In progress | CI provisions disposable Postgres, runs migrations, fixtures, seeds, Python tests, dbt parsing, materializes the public BICAM graph plus reconciliation, reciprocity, coverage, and source-gap evidence, runs the fixture-safe dbt assertion selector (77 tests in the current disposable run, including resolver-backed bill and amendment action, bill title, subject/summary, member-legislation, committee-report membership, amendment HTML Congressional-Record exclusion, amendment reciprocity observations, query-parameter collection normalization, immutable child-version lineage, amendment-cosponsor staging reduction, nomination same-position preservation, amendment-text source-position preservation, committee-report baseline/delta lineage, bill/amendment action and edge keys, committee nomination/communication and committee-print text natural-key preservation, nomination action/committee/hearing/activity natural-key preservation, treaty action/committee/activity natural-key preservation, bill-amendment and both committee↔bill membership grains, non-empty vote-key, bill-action committee-edge, related-bill-edge, non-paginated child completeness, reciprocal bridge checks, fail-closed reciprocity metrics, missing-Congress committee-report probe construction, and complete-empty child replacement), and runs the 91-relation row-level validator. It also exports a complete supplemental candidate, validates/promotes that exact candidate only inside the isolated export directory, performs an exact self-comparison through `scripts/compare_release_candidates.py`, and rejects a copied/hash-drifted candidate while checking that the accepted pointer is unchanged. The current disposable graph contains 233 public BICAM models, 237 including reconciliation, and 242 including release-quality evidence; the source-removal slice passes 1/1 on the 42-payload fixture corpus, the title workload passes its 5,000 ms threshold, and the expanded candidate lifecycle now passes. |
The CI fixture selector now also includes the direct committee-print rendition
assertion; its prior 51-test evidence remains valid, with the new shape check
covered separately at 2/2 in the disposable graph.

| H — reproducibility/version evidence | Partial | `bills_versions` and `bills_version_links` are implemented and validated as supplements. Candidate manifests now carry explicit source-bundle/dependency/environment identity through the provenance sidecar; supplemental promotion fails closed if that evidence is absent. The bounded `scripts/export_source_evidence.py` exporter now creates content-addressed payload subsets with checksummed manifests; full candidate bundle binding, row-level source traces, and frozen-input recreation are still needed. |
| I — research limitations/documentation | Partial | A researcher-facing guide now documents the eleven plural root families, canonical union/source policy, stable identifiers, bill-version and HTML-text usage, reciprocity interpretation, source-gap semantics, and tested-style loading/query examples. Coverage denominators, independently adjudicated linkage metrics, manual text evaluation, and bundling the guide with a validated release remain outstanding. |

## Work completed in this integration

The incremental child fix exposed a downstream grain error in
`stg_congress__bill_cosponsors`: the complete 119th snapshot plus two
non-overlapping deltas contained one repeated member. The staging relation was
therefore larger than the bill detail's authoritative count. The fix adds:

- `int_congress_bill_cosponsor_observations`, a materialized evidence relation
  retaining source raw ID, fetch time, endpoint, array ordinal, and parsed
  fields for every selected occurrence;
- deterministic latest-observation reconciliation in the existing staging
  column shape at `(bill_id, bioguide_id)`;
- a uniqueness regression test and refresh-script selectors.

The affected acceptance checks now pass against the operational database:

```text
child_full_snapshot_precedes_incremental_empty_response       PASS
incremental_child_windows_are_accumulated                     PASS
bill_cosponsors_have_one_current_observation                   PASS
bills_titles_have_one_row_per_semantic_key                      PASS
bills_version_links_title_codes_match_source                    PASS
```

The propagation regression suite now also covers the previously missing
distinction between an empty incremental poll (preserve prior state) and an
empty complete snapshot (replace prior state). The affected dbt models compile
with the revised macro; the targeted database materialization and its six
focused dbt tests have now completed successfully.

The materialized evidence contains 4,097,948 source occurrences and the
reconciled staging relation contains 4,092,972 current bill/member rows with
zero duplicate keys. The audited fixtures now match their authoritative
counts: `hconres69-108` = 10 and `hr1266-119` = 113.

The amendment-side reciprocity path had one remaining bypass of the child
resolver: it expanded only `int_raw_responses__current` for amendment
cosponsors. That path now materializes
`int_congress_amendment_cosponsor_observations` with the same complete
generation, delta, raw-version, crawl-run, page, and array-ordinal evidence as
the bill-cosponsor and nominee relations. The reciprocity edge model consumes
that relation. A disposable seeded dbt run materialized the nine-model
dependency slice successfully (`PASS=9 WARN=0 ERROR=0 SKIP=0`), and the
incremental propagation unit suite now has ten passing tests.

The Congress bill text-version expansion had the same latest-URL shortcut.
`int_congress_bill_text_versions` now resolves `bill_text` child windows before
expanding `textVersions`, preserving earlier non-overlapping windows for the
downstream HTML/text and version-link lanes. Its disposable four-model
dependency slice completed successfully (`PASS=4 WARN=0 ERROR=0 SKIP=0`).

The committee-report text expansion had the same class of bypass. The text
format model now resolves `committee_report_texts` through the child-state
macro, joins the selected windows to their owning report-detail response, and
retains both child and detail raw-version evidence. Its report-detail parent
ID is now carried through the intermediate relation. A static propagation
regression covers the resolver call and parent lineage. The isolated
eighteen-payload database slice materialized the 12-model text dependency
graph (`PASS=12 WARN=0 ERROR=0 SKIP=0`); the new rendition test proves that the
complete response and later delta retain one PDF plus original and errata HTML
formats with their child/detail lineage.

The child resolver itself also had a shape gap: its completeness detector only
looked for arrays directly under the response root, but Congress bill subjects
nest `legislativeSubjects` below `subjects` and some child payloads expose more
than one top-level collection. The resolver now recursively discovers nested
arrays while preferring direct collection arrays when present. Bill subjects
and summaries now reconcile by their declared public keys (`bill_id, subject`
and `bill_id, version_code`) with latest-observation precedence. A seven-payload
disposable slice materialized the ten-model dependency graph (`PASS=10`) and
both key tests passed; the resulting public fixture rows were one subject and
one summary after duplicate observations were collapsed.

The reciprocity metrics are now explicitly family-complete: all eleven
declared Congress.gov relationship families receive an aggregate row, even
when a current scope has zero observed edges. Such a zero-count family is
marked `review` rather than disappearing or producing a null rate. The same
isolated 43-model slice produced all 11 metric-family rows. A dedicated
dbt assertion and a Python declaration test keep the fact, metrics, and audit
coverage list aligned; the new assertion passed in the disposable database
(`PASS=1 WARN=0 ERROR=0 SKIP=0`). This makes missing relationship families
observable in release evidence instead of allowing an empty aggregate to pass
vacuously.

The public contract is now version 3. It removes the false
`(nomination_id, ordinal)` primary-key declaration from `nominations_nominees`,
declares that pair as a grouping key, and records a deterministic export order
without adding provenance columns to the public archive surface. It also
honestly treats bill and amendment text renditions as grouping-key relations:
historical source rows can have null dates or multiple rendition URLs for the
same `(entity_id, type)` grouping. The contract
validator, published-archive compatibility check, export dry-run tests, and
nominee contract regression all pass.

As the next Package C hardening step, the `amendments` public mart now ranks
repeated raw detail observations by source update/version evidence before
joining the timestamp. This preserves the public one-row-per-`amendment_id`
grain even when retries or overlapping windows produce duplicate detail rows;
`amendments_have_one_row_per_key` makes that invariant explicit. The changed
model compiles successfully, and the read-only current-table assertion passed
(`PASS=1 WARN=0 ERROR=0 SKIP=0`); a production table rebuild is intentionally
deferred to the isolated candidate pipeline.

The first live row-level audit found five issues: legitimate duplicate
historical bill/amendment text renditions under an insufficient
`(entity_id, date, type)` key, 41,537 bill-text rows with source-null dates,
and four source-null hearing titles. Contract version 3 records the honest
grouping and nullable semantics instead of deleting renditions or inventing
values. The corrected audit then passed all 506 row-level checks, including
required values and foreign keys, with zero failures; its JSON artifact is
`reports/public-contract-data-validation-latest.json`.

Focused implementation tests and lint checks also pass: the incremental
propagation suite now has 10 passing tests, and Ruff passes for the changed
implementation/test paths.

The complete local Python suite subsequently completed with `389 passed,
5 skipped`; the skips are the explicitly guarded storage integration tests
without a disposable `BICAM_TEST_DATABASE_URL`.

The latest Package B follow-up moved related-bill expansion into
`int_congress_bill_related_bill_observations`, retaining source-version,
generation, page, and array-position evidence before the public natural-key
projection. Its disposable graph completed 3/3 models, all eight observation
schema tests passed, and `bills_related_bill_keys_are_preserved` passed.

The latest Package D follow-up adds executable support for composite
foreign-key declarations without changing the existing public contract. A
focused validator/propagation run passed 29 tests, the full local Python suite
passed `389 tests with 5 guarded storage skips`, and Ruff plus `git diff
--check` passed. Existing scalar references and the 506-check candidate
validation remain unchanged.

The resolver follow-up also added a regression for non-paginated child arrays.
After a full `+staging` build of the disposable graph (`173` models, no
errors), the expanded fixture-safe selector passed `51/51`. The new assertion
ensures that a child response with no pagination object is not incorrectly
marked incomplete, while the related-bill and bill-action observations still
retain their source-version, page, generation, and array-position evidence.

The HTML progress reporter now separates source absence (`404`/`410`), other
HTTP errors, transport/fetch errors, and empty extraction from the aggregate
non-success count. This makes the extraction report usable for the Package I
coverage/evaluation denominator instead of treating every unsuccessful attempt
as the same kind of missingness. The focused HTML/report test slice passes
13 tests; the running extractor will publish the expanded fields on its next
lock-free checkpoint.

The first full operational reconciliation rebuild after the fixture work found
that the amendment recorded-vote mart's unique index omitted `action_id` even
though its SQL grain and contract include it. Congress 106 has distinct actions
sharing Senate roll 127, so the old four-column index failed closed during
materialization. The index now uses the declared five-column key
`(action_id, chamber, congress, session, roll_number)`, matching the bill vote
mart; a static regression guard and dbt parse pass. The already-running build
will retain its failure result for that model, and the next lock-owned refresh
will apply the corrected definition.

That refresh completed with `PASS=267 WARN=0 ERROR=1 SKIP=0`; the wrapper's
separate freshness check passed, but the 09-04 gates correctly treat the mixed
DBT result as non-certifying. The corrected five-column definition is verified
on the disposable fixture database and remains queued for the next serialized
full operational rebuild. The 00:57 UTC lineage repair completed successfully;
the HTML lane is waiting behind the same shared lock. No release pointer was
promoted.

The disposable fixture corpus now includes distinct bill and amendment actions
that intentionally reuse the same roll number. This makes the failure mode
non-vacuous in CI; the fixture-loader and propagation slice currently pass
20 tests. A refreshed disposable amendment-vote slice materialized 5/5 models,
produced four rows for two actions sharing rolls 354/355, and passed
`amendments_recorded_vote_keys_are_preserved`.

The storage test guard was tightened separately: without an explicit
`BICAM_TEST_DATABASE_URL`, its five database tests skip rather than touching
the operational database, while two pure guard tests verify that
`DATABASE_URL` cannot activate or redirect them. CI remains configured with a
disposable PostgreSQL DSN and is the authoritative integration proof.

The source-gap report now recalculates `due_for_reprobe` and its next action
from `expires_at` when it reads the durable queue. An item therefore cannot
remain deferred merely because the last dbt table refresh predates its expiry;
the Pushover path remains opt-in and credential-free.

The child resolver now also carries `raw_response_version_id`,
`latest_crawl_run_id`, page-normalized `logical_endpoint`, offset-normalized
`logical_collection`, and array-state flags into its selected evidence. The
bill-cosponsor and nominee observation relations retain the exact raw-version
and crawl-run IDs for every selected array occurrence. The collection key is
currently audit/reporting metadata. It now also carries page offsets and a
generation completeness proof. A generation is complete only when its crawl
run, API-reported total, single-array item counts, and page intervals prove
contiguous coverage from offset zero; otherwise the prior page-level fallback
is retained and marked incomplete. The new observation-level audit passed for
all selected complete generations: 3,772,955 bill-cosponsor observations and
523,680 nominee observations are covered by proven generations. The known
`hres932-117` split history remains explicitly incomplete (94 observed of an
API-reported 95), so it is not silently promoted to a complete replacement.
The detailed checkpoint is
`reports/incremental-generation-checkpoint-20260905T145620Z.md`.

The bounded HTML lane continues at bounded concurrency. The 00:06 UTC
checkpoint now reports 331,504 bill-text targets (34,129 successful), 290,350
bill-version targets (38,749 successful), 334 committee-print targets, 19,714
committee-report targets, and 36,535 hearing targets (36,523 successful).
The 50,057 distinct Congressional Record links carried by
`amendments_texts.html` are explicitly excluded from fetching and public text
joins because Congressional Record is paused. The remaining bill-text and
bill-version backlog is 286,884 and 250,100 respectively; PDF/XML links remain
excluded. The operational amendment mart now retains 84,582 source links but
has zero Congressional Record rows with non-null public prose. The cursor and
append-only versions are durable, the six source
tables remain auditable, and the two HTML integrity tests passed in the
serialized cycles. The lane continues on its normal bounded schedule;
its corrected wrapper now holds the shared lock through extraction, DBT, tests,
and progress reporting. The user service now waits up to 15 minutes for that
lock (still bounded and memory/API pacing unchanged), rather than abandoning
most ten-minute ticks after 120 seconds.

The disposable text-shape audit found that live Congress.gov committee-print
payloads publish rendition objects directly under `text`, whereas the
intermediate model only expanded nested `formats` arrays. That mismatch had
left the operational `committee_prints_texts` relation empty despite harvested
URLs. The intermediate now accepts both shapes, the fixture includes a direct
rendition, and the disposable graph materializes four source renditions and
one public print row. Both the nested-format and direct-rendition dbt tests
pass; the corrected definition is awaiting the next serialized operational
refresh.

The first lock-free progress checkpoint at 01:25 UTC now separates 10,468
bill-text HTTP errors from 23 fetch errors, 1,500 bill-version HTTP errors
from one fetch error, and 12 hearing fetch errors. The reporter was then
hardened to retain an explicit zero row for every declared text source, so an
empty or temporarily unavailable public relation cannot disappear from the
coverage denominator on the next checkpoint.

The subsequent 01:36 UTC artifact confirms the behavior: all six declared
sources are present, including `committee_prints_texts` with zero current
targets while its operational model awaits the serialized refresh. The same
checkpoint records 250 additional successful bill-text fetches and 250
additional successful bill-version fetches.

The 01:46 UTC artifact records another 250 successful bill-text and 250
successful bill-version fetches. A live read-only quality-gate audit also
passed with no stale Congress runs, unsatisfied child fetches, open
reconciliation exceptions, or non-pass reconciliation metrics.

The 01:49 UTC lineage checkpoint again passed repair and freshness checks; the
HTML service subsequently reacquired the shared lock and resumed its next
bills-text batch.

The 01:56 UTC HTML artifact records successful completion of the bills-text
and bills-version batches (250 each), followed by the text-model propagation
and integrity tests. Its cursor has rotated to `amendments_texts`; committee
prints remain at zero targets until the serialized Congress refresh.

The extractor tests now derive HTML-bearing relations from the public contract
and require each to be registered in the extraction lane, preventing a new
text-bearing relation from being silently omitted.

The six-hour Congress supplemental wrapper now runs both committee-print text
shape assertions alongside its reconciliation tests, so the next operational
refresh will fail closed if direct renditions are lost again.

The wrapper now also emits `quality_gate_status` and a combined
`combined_release_gate` result. A DBT failure can no longer be accompanied by
an ambiguous standalone “QUALITY GATE PASSED” line in the checkpoint; the
combined result remains `FAIL` unless DBT, targeted tests, and the quality gate
all succeed.

The GovInfo incremental-window checkpoint at
`reports/govinfo-window-checkpoint-20260906T011751Z.md` completed with status
zero through the current Congress 119. All four bounded collections had zero
failed fetch events; the nested identity audit, targeted reconciliation build,
and seven bill-version/source-gap tests passed. The latest bounded window
completed at 01:17 UTC; lineage/freshness also continues to report
`repair_status=0` and `freshness_status=0`.

During the post-refresh reciprocity check, the narrow edge rebuild exposed a
coordination defect in several scheduled wrappers: a standalone `flock 9`
only held the lock for the helper process, not for the enclosing shell script.
The affected HTML, lineage, Congress/GovInfo refresh, audit/report, snapshot,
nomination, retry, and reconciliation wrappers now re-exec under a held lock
with an explicit conflict exit code. Shell syntax checks pass. The currently
running legacy HTML invocation was allowed to finish; subsequent timer runs
use the corrected serialization. A live subshell contention test confirmed
that the corrected primitive blocks a second lock attempt. This is an
operational correctness fix, not evidence that the corrected release is
promoted.

The current live contract validator (including both bill-version supplements)
reports 91 relations with no schema mismatch, and the quality-gate command
passes. Those are current-database checks, not a candidate-release acceptance
result; the remaining C-E work is to run the complete exporter against a
disposable/frozen build and retain its candidate-bound evidence, not to rely
on `latest` or a live post-export check.

The CI fixture loader now includes representative `bill_text`, nested
`bill_subjects`, `bill_summaries`, non-empty bill/amendment action payloads,
reciprocal committee/bill payloads, duplicated bill→amendment and
committee→report child edges, duplicate bill/amendment action and
amendment-to-amendment observations,
member-side sponsored/cosponsored legislation edges, committee nomination/
communication edges,
committee-print text renditions, a committee-report text baseline plus
incremental window linked to its detail record, duplicate bill-title
observations, and the committee-report membership duplicate. This keeps
resolver-backed child paths, relationship grains, and reciprocity in the
disposable graph rather than testing them only through static SQL assertions;
the fixture corpus is now forty payloads. The loader preserves
`parent_id` in both current and immutable raw-version inserts. Its changed
Python paths pass Ruff, and the complete local suite is `389 passed, 5
skipped`. The follow-up forty-payload disposable smoke materialized the
subject/summary, vote, bill-action committee, related-bill, bill-amendment,
and committee-report/committee-print text dependency graphs; the selected
relationship run passed, the committee-report text graph completed 12 models
successfully and the committee-print text graph completed 3 models, the
bill-amendment graph completed 3 models successfully, and the vote,
committee-edge, related-edge, bill-amendment, committee-report membership,
committee-report rendition, bill/amendment action, amendment-action committee,
amendment-to-amendment, and member-legislation,
committee-nomination, committee-communication,
committee-print rendition, nomination-action, nomination-action-committee,
nomination-hearing, nomination-activity, treaty-action, treaty-committee, and
treaty-activity
preservation/lineage tests passed.
The public
tables
contained two bill rolls, two amendment rolls, one deduplicated `HSAG`
committee edge, and one `hr1-118` → `hr2-118` related-bill edge. The
natural-key tests also passed, producing one deduplicated subject and one
summary. The 42-model reciprocity slice also passed, with both committee↔bill
directions retained and classified as `reciprocal`. The durable reciprocity
checkpoint is
[reciprocity-committee-bill-checkpoint-20260905T200734Z.md](../reports/reciprocity-committee-bill-checkpoint-20260905T200734Z.md).
The bill-action committee checkpoint is
[action-committee-grain-checkpoint-20260905T195823Z.md](../reports/action-committee-grain-checkpoint-20260905T195823Z.md).
The related-bill checkpoint is
[related-bill-grain-checkpoint-20260905T200215Z.md](../reports/related-bill-grain-checkpoint-20260905T200215Z.md).
The durable vote-grain checkpoint is
[vote-grain-checkpoint-20260905T194850Z.md](../reports/vote-grain-checkpoint-20260905T194850Z.md).
The bill-amendment checkpoint is
[bill-amendment-grain-checkpoint-20260905T202526Z.md](../reports/bill-amendment-grain-checkpoint-20260905T202526Z.md).
The bill-committee checkpoint is
[bill-committee-grain-checkpoint-20260905T202845Z.md](../reports/bill-committee-grain-checkpoint-20260905T202845Z.md).
The committee-bill checkpoint is
[committee-bill-grain-checkpoint-20260905T203056Z.md](../reports/committee-bill-grain-checkpoint-20260905T203056Z.md).
The committee-report text checkpoint is
[committee-report-text-delta-checkpoint-20260905T201803Z.md](../reports/committee-report-text-delta-checkpoint-20260905T201803Z.md).
The committee-report membership checkpoint is
[committee-report-membership-grain-checkpoint-20260905T204816Z.md](../reports/committee-report-membership-grain-checkpoint-20260905T204816Z.md).
The nomination-action checkpoint is
[nomination-action-grain-checkpoint-20260905T205336Z.md](../reports/nomination-action-grain-checkpoint-20260905T205336Z.md).
The treaty committee checkpoint is
[treaty-committee-grain-checkpoint-20260905T210707Z.md](../reports/treaty-committee-grain-checkpoint-20260905T210707Z.md).
The treaty action, committee-child, and committee-print text checkpoint is
[child-natural-key-expansion-checkpoint-20260905T212000Z.md](../reports/child-natural-key-expansion-checkpoint-20260905T212000Z.md).
The combined CI graph-selector checkpoint is
[ci-relationship-graph-checkpoint-20260905T203747Z.md](../reports/ci-relationship-graph-checkpoint-20260905T203747Z.md).
The disposable database was used only for this CI-style check; no operational
database or release pointer was modified.

The first isolated full-graph CI attempt intentionally ran before seeds were
loaded and mixed the complete model path with every project data test. It
produced four missing-seed relation errors and thirteen sparse-fixture test
failures. CI now loads all 16 bundled seeds and runs the complete model graph
as a model-only materialization step; the same 512 MB disposable PostgreSQL
run then completed all 224 selected models successfully. This separates graph
compilation/materialization evidence from assertions that require the
representative adversarial corpus still required by Packages A, C, and G. The
former broad focused selector reached 702 passes, 137 explicit skips, and 7
errors on the sparse corpus; those errors were expected fixture coverage gaps
(root partitions, reviewed GovInfo references, nomination parts, and a bridge
outside the small sparse corpus), not model compilation failures. CI now uses an
explicit thirty-six-test fixture-safe subset (36/36 pass) plus the 91-relation
row-level validator. The broader source-coverage selector remains a
release-candidate check until the adversarial fixture matrix is expanded. The
workflow's graph selector now also includes `+fct_reconciliation_metrics` and
`+bridge_committees__bills`; without those explicit ancestors, four of the
fixture assertions would fail at database-setup time because their relations
were absent rather than because the assertions found bad rows.

The CI workflow now exercises the candidate lifecycle itself: after the
disposable graph and row validator pass, it writes a complete core-plus-
supplemental candidate, validates and promotes that exact candidate inside
`$RUNNER_TEMP`, then copies it, injects manifest/hash drift, and confirms the
negative candidate cannot validate or displace the accepted temporary pointer.
The workflow remains isolated from the operational export directory.

The corrected full-database candidate was then exercised with the same
release path in `/tmp/bicam-candidate-check-20260905c`. The exporter wrote 91
core-plus-supplemental relations at
`20260905T192444Z`, bound them to build
`058329228cd79dfaf2e48fcbbca91ec4fedb598dc53db5fc44fc622d3f26edbe`, and
recorded `status=complete`, `quality_status=pass`, zero scoped actionable
missing details, zero unsatisfied child fetches, three passing
reconciliation rows, and 506/506 passing row-level checks. The exact manifest
passed dry-run validation and temporary promotion; the temporary `latest`
pointer selected that candidate. A hard-linked copy with a changed snapshot
directory and a zeroed table digest failed closed (`artifact bytes differ from
manifest`) while leaving the accepted pointer unchanged. This validates the
09-04 candidate-bound release interface without promoting the operational
release directory.

The durable checkpoint is [disposable-candidate-checkpoint-20260905T192444Z.md](../reports/disposable-candidate-checkpoint-20260905T192444Z.md).

The first full current-database candidate exposed an over-broad release check:
four missing details belonged to the paused `house-communications` category,
not the eleven-family Congress scope. The exporter now applies
`TARGET_CONGRESS_CATEGORIES` to root and child coverage evidence, matching the
release freshness and manifest checks. The scoped live total is zero while the
out-of-scope four remain visible in the operational coverage fact.

The original root-level architecture sketch and the August remediation review
now carry explicit supersession notices. They remain useful as historical
context and measured findings, but agents must use the September 4 A–I plan
and this status document for scope, ownership, and release criteria. This
prevents the old 13-category/"brand new implementation" wording from being
treated as the current goal.

The 2026-09-05 Congress supplemental checkpoint then completed successfully:
the 256-model build passed, all 59 targeted dbt tests passed, and the quality
gate passed. The refreshed reciprocity fact contains 5,402,369 unioned edges
across 11 relationship families; `bicam.bills_versions` contains 290,350
version rows and `bicam.bills_version_links` contains 826,630 source links.
Both evidence relations contain complete lineage coverage (4,097,948 bill
cosponsor observations and 1,289,126 nominee observations). The local Python
suite is now `389 passed, 5 skipped`, and Ruff passes. The refreshed queue
report contains 358,912 rows, of which 23 are currently due for reprobe; no
Pushover notification was sent. The current artifact is
`reports/source_gap_queue/latest.md` (22:32 UTC); Pushover remains disabled
until credentials and an explicit enablement decision are supplied.

The preceding 09-04-plan disposable integration rerun used 40 payloads and
completed the public BICAM graph at 233 models, the public/reconciliation graph
at 237 models, and the release-quality graph at 242 models with no dbt errors.
The initial expanded fixture-safe selector passed 50/50 tests; the subsequent
full-staging refresh added the non-paginated-child assertion and passed 51/51.
The public contract validator passed all 506 row/schema checks, including the
supplemental bill version relations. During that run, nomination detail payloads with a
count/link-shaped `nominees` field were safely distinguished from true arrays;
committee detail name/chamber values now fall back to the detail object when a
list payload is not present, so valid sparse fixtures do not become null public
records. These are fixture and source-shape safeguards, not a release promotion.

The next Package-B caller pass moved bill-amendment and bill-committee
membership expansion behind `int_congress_bill_amendment_observations` and
`int_congress_bill_committee_observations`. Both retain raw-response-version,
crawl-run, endpoint, page, generation, and source-array ordinal evidence; the
existing staging relations still publish their declared natural keys and
columns. The disposable six-model graph completed with no errors, and the two
new lineage assertions plus both existing membership-key assertions passed
(`PASS=4/4`).

The reciprocal committee→bill caller now follows the same boundary through
`int_congress_committee_bill_observations`; `stg_congress__committee_bills`
keeps its public columns while reducing by `(committee_id, bill_id)`. Its
disposable graph and the lineage, natural-key, and canonical committee↔bill
bridge assertions passed (`PASS=3/3`).

The treaty-action caller now follows the same observation-first boundary
through `int_congress_treaty_action_observations`; `stg_congress__treaty_actions`
keeps its public columns while reducing by `action_id`. Its disposable
three-model graph completed without errors, and the lineage plus treaty-action
key assertions passed (`PASS=2/2`).

Treaty committee and committee-activity staging now share
`int_congress_treaty_committee_observations`, so the selected child windows are
expanded once while retaining raw-version, crawl-run, generation, endpoint,
page, and array-position evidence. The disposable four-model graph completed
without errors, and the lineage plus committee/activity key assertions passed
(`PASS=3/3`).

Committee communication and committee nomination staging now consume separate
resolver-backed observation relations retaining parent/detail lineage,
raw-version, crawl-run, generation, endpoint, page, and source-array position
before their public edge-key reductions. The combined disposable graph
completed 6/6 models, and the two lineage plus two public-key assertions passed
(`PASS=4/4`).

Nominee and nominee-detail staging now consume the existing
`int_congress_nominee_observations` relation instead of independently expanding
the raw child URL. The observation carries parent/detail raw lineage for both
public projections; the disposable four-model graph and three nominee
grain/source-preservation assertions passed (`PASS=4/4` models, `PASS=3/3`
tests).

The nomination action, hearing, and committee-activity callers and the
amendment-to-amendment caller have also moved behind materialized
resolver-backed observation relations. The final static inventory has no
direct `resolve_child` call in an in-scope Congress.gov staging model; only the
three paused House requirement/vote families retain direct calls.

Incomplete selected child pages are now surfaced through
`fct_child_state_gaps` and unioned into `fct_source_gap_queue` as
`needs_source_refresh`/`reprobe` items. The disposable child-state graph
completed without errors, and the child-gap plus open-queue action assertions
passed (`PASS=2/2`); all 23 queue/contract schema tests also passed.
The current fixture exposes two incomplete child collections
(`committee_print_texts` and `committee_report_texts`); transport/page/date
variants are normalized to one open `reprobe` item per logical collection
rather than being treated as complete.

The nomination-part preservation fixture guard now matches the maintained
`pn123-01-118`/`HSAG` disposable corpus rather than an obsolete `pn726`/`ssju00`
expectation; the guard passes after the complete nomination child slice is
materialized.

The candidate exporter now binds supplemental release evidence to an explicit
source allowlist. Its source-bundle metadata records the BICAM Git revision and
dirty-worktree digest, the allowlisted source-tree digest, `uv.lock` hash, the
local Rotisserie revision/tree digest when available, and runtime versions; it
does not include `.env`, credentials, caches, or raw payloads. The provenance
sidecar mirrors this evidence, and supplemental candidate validation rejects a
missing or malformed bundle. Core-only candidate fixtures remain backward
compatible. The focused exporter/promotion regression suite passes 24/24 tests
and Ruff passes for the changed paths; no operational export or release pointer
was changed.

## Next implementation order

1. Rerun the complete candidate export, validation, promotion, and
   self-comparison on the expanded 42-payload fixture corpus, then bind the
   corrected production export to the exact build/metric/frontier evidence.
2. Use the completed 17:28 operational generation as the source for the
   candidate-bound export. Its committee-report probe endpoint, GovInfo-only
   exception queue, corrected amendment-vote key, bill-version links, and
   optimized title/source-gap graph all passed the targeted runtime checks;
   the export still must bind those results to one immutable build.
3. Complete H/I research-ready evidence: distributable source-payload bundle,
   frozen-input recreation, generation IDs, and evidence-based coverage,
   linkage, text, and researcher documentation. Keep public BICAM relations
   provenance-light while retaining supplemental evidence artifacts.
4. Continue the bounded HTML extraction lane in parallel, reporting source
   absence, fetch failure, extraction failure, empty text, and successful
   extraction separately. PDF/XML extraction and deferred external crosswalks
   remain out of scope.

## Operational constraints

The HTML extractor, lineage audit, and scheduled source refreshes continue
under the shared `/run/user/1000/bicam-ingestion.lock`; this document does not
authorize interrupting them or promoting a release. No Pushover credentials
are configured. PDF/XML extraction and deferred external lobbying/Voteview
crosswalks remain out of scope for this release.

## Live evidence after plan integration (2026-09-06 09:35 UTC)

The first scheduled HTML checkpoint after the cadence change completed two
useful batches: 250 Congress bill-text renditions and 250 GovInfo bill-version
renditions, all reported successful. Its durable checkpoint records
`dbt_batches_since_last=2` and `pending_dbt=1`; propagation was deferred until
the source-table cycle boundary, where it then started under the unique
`bicam-html-dbt-20260906T093534Z-3984860` application name. This confirms the
new batching rule and selective cancellation path. The bounded propagation
reached its 600-second client bound (`html_dbt_run_status=124`), was cancelled
by that exact application name after the client exited, and retained
`pending_dbt=1` with `dbt_batches_since_last=0`; the fetched rows and cursors
were preserved.

The 09:29 GovInfo window fetched all four bounded categories through
09:04:34 UTC with zero failed fetch events. Its targeted reconciliation graph
timed out at the 900-second bound (`status=124`) and the wrapper failed closed;
the exact checkpoint is
`reports/govinfo-window-checkpoint-20260906T092954Z.md`. No release pointer
moved, and the timeout is not a quality pass.

The local regression suite remains `425 passed, 5 skipped`; Ruff, shell syntax,
and `git diff --check` pass for the changed paths. The active goal therefore
remains open under the 09-04 A–I acceptance contract.

## Package-B transport-scan optimization (2026-09-06 09:52 UTC)

The reusable `int_congress_child_response_selection` model now uses an
incremental `delete+insert` materialization keyed by immutable
`raw_response_version_id`. It revisits new versions, changed last-seen/current
fetch evidence, and repaired parent IDs, while leaving the downstream
`int_congress_child_response_selections` relation as a full table rebuild so a
complete replacement can still retire rows. The JSON-shape statistics are
also grouped by version ID rather than URL/raw ID, preventing different
payload versions of one endpoint from contaminating each other's array facts.

Against the disposable 42-payload PostgreSQL corpus, the incremental run
completed with `INSERT 0 0`, a full-refresh run selected 42 rows, the selected
state table rebuilt 28 rows, and the five child-state assertions passed. The
local Python suite is now `425 passed, 5 skipped`; Ruff and dbt parse pass.
The next serialized operational refresh will be the first production-scale
exercise of the incremental transport path; no operational database was
manually rebuilt in this change.

The HTML wrapper now uses an explicit 32-model text/child-state graph rather
than `+`-selecting every ancestor of each public text relation. This retains
the required raw/current, resolver, text-specific, identity-map, and public
relations while excluding unrelated bill/title/reconciliation rebuilds. The
disposable graph completed all 32 models in 1.21 seconds; its first
production-scale run is queued behind the current GovInfo lock.

## Latest operational checkpoint (2026-09-06 10:22 UTC)

The serialized GovInfo window completed all four bounded categories through
09:57:01 UTC with one list page and one fetch event per category and zero
failed fetches. The current Congress resolved to 119, and the bill/hearing
counterpart probes, nested identity audit, and targeted reconciliation test
all returned status 0. The targeted reconciliation build itself reached its
900-second bound (`status=124`), so the wrapper failed closed and did not
advance a release pointer. The durable artifact is
`reports/govinfo-window-checkpoint-20260906T102241Z.md`.

After the lock was released, the HTML lane resumed the next 250-item bill
text batch under the patched explicit graph and completed it successfully at
10:27 UTC. Its durable checkpoint now records the new bill-text cursor,
`dbt_batches_since_last=1`, and `pending_dbt=1`; propagation remains deferred
until the configured batch boundary. At the preceding 10:01 checkpoint,
bills had 42,126
successful renditions and 278,884 pending targets, while GovInfo bill
versions had 46,749 successful renditions and 242,100 pending targets. The
rolling estimates were approximately 394 and 342 hours respectively at the
observed 708 requests/hour cadence. These are extraction estimates, not
release-completion estimates; committee prints, committee reports, and
hearings were already complete or effectively complete for HTML extraction.

The current local validation is `425 passed, 5 skipped`; Ruff, all three
scheduled-wrapper syntax checks, and `git diff --check` are clean. The active
goal remains open: this evidence advances operational confidence but does not
close the 09-04 A–I candidate, portable-evidence, coverage, linkage, and
research-documentation gates.

## Follow-up transport-index optimization (2026-09-06 10:41 UTC)

The selected child-state table now carries explicit composite indexes for its
two production access paths: generation proof windows
(`category/node_type/logical_collection/crawl_run/incremental/page_offset`) and
latest-page fallback selection (`category/node_type/logical_endpoint/fetched_at`).
The disposable PostgreSQL fixture rebuilt the table successfully (`SELECT 28`)
and verified both indexes plus the existing version-lineage indexes. The new
regression guard is included in the full local result of `425 passed, 5
skipped`.

The 10:38 UTC HTML checkpoint added 250 successful Congress bill-text and 250
successful GovInfo bill-version renditions. Its explicit propagation graph
reached the 600-second bound (`html_dbt_run_status=124`), failed closed, and
left `pending_dbt=1` with the fetched cursors intact. The durable progress
artifact reports 42,376 successful bill texts with 278,634 pending targets and
46,999 successful bill versions with 241,850 pending targets. The rolling
sweep estimates are approximately 418 and 363 hours at the observed 667
requests/hour rate; these are extraction estimates, not release-completion
claims.

## Candidate-bound plan integration checkpoint (2026-09-06 11:13 UTC)

The first current-database supplemental candidate export after the 09-04 plan
integration was completed from one repeatable-read snapshot. It wrote 91
relations (the 89 core relations plus `bills_versions` and
`bills_version_links`) and 32 source frontiers to
`/tmp/bicam-candidate-current-20260906T1106Z/20260906T110620Z/`. The manifest
contains build ID
`90e98c521d6c04d76c92247870eea0a624526a9e94988527db36229c898cacf8` and
505 valid of 506 row-level checks.

The exact supplemental dry-run validator rejected this candidate with
`candidate build evidence is not complete`; no `latest` pointer or production
table was changed. The concrete contract failure is the foreign-key check
from `committee_reports_associated_bills.report_id` to
`committee_reports.report_id` for `hrpt770-1-119` and `hrpt782-1-119`.
Both report IDs are present as bill-side-only observations in
`int_committee_reports__congress`, but the currently materialized public root
table lacks them. This is a mixed/stale-generation defect, not evidence that
the valid bill/report edges should be removed.

This result applies the 09-04 gates directly: Package D/E correctly fail
closed on a candidate-bound relational inconsistency, while Packages B/C
remain responsible for preserving the bill-side union and honest public
grain. The next serialized reconciliation build must refresh
`committee_reports` and `committee_reports_associated_bills` in the same
generation, then repeat export, row/file validation, comparison, and (only if
all A--G gates pass) isolated promotion. The forensic candidate and details
are retained in
[`candidate-current-export-checkpoint-20260906T1106Z.md`](../reports/candidate-current-export-checkpoint-20260906T1106Z.md).
Packages H--I remain downstream of that corrected-release evidence; PDF/XML
parsing and Pushover delivery remain out of scope.

## Serialized root/edge repair (2026-09-06 11:24 UTC)

The candidate failure exposed a concrete scheduling omission rather than a
source-data problem: the hourly GovInfo reconciliation selected the report
intermediates and match facts but not the public `committee_reports` root or
`committee_reports_associated_bills` edge. The wrapper now materializes both
relations in the same reconciliation invocation and runs
`committee_reports_public_union_preserves_bill_side` plus
`committee_reports_associated_bills_cover_internal_union` afterward. The
focused wrapper test passes (`8 passed`), shell syntax is valid, and `dbt ls`
resolves all four selectors. The live database remains non-certifying until a
scheduled run completes with these selectors and a fresh candidate validates.

The follow-up validation after this wrapper change is `426 passed, 5 skipped`
for the full local Python suite; the five skips remain the explicitly guarded
storage checks without a test DSN. The focused propagation/wrapper/HTML slice
is `45 passed`.

## Post-fix scheduled-window result (2026-09-06 11:22 UTC)

The first hourly GovInfo run after the selector repair completed its four
bounded source windows with zero fetch failures, resolved Congress 119, and
passed the nested-identity and targeted-test checks. Its reconciliation graph
materialized the report-side intermediates, bill-version supplements, and
metrics through model 72/75, then reached the 900-second bound
(`targeted reconciliation status=124`). The run therefore failed closed and
did not advance a release pointer. The authoritative artifact is
[`govinfo-window-checkpoint-20260906T112207Z.md`](../reports/govinfo-window-checkpoint-20260906T112207Z.md).

The public root still lacked `hrpt770-1-119` and `hrpt782-1-119` because this
attempt began before the selector change was installed; the next scheduled
window is the first one that will materialize both public report relations and
their assertions together. The HTML extractor remains serialized behind the
shared lock and continues its durable bill-version batch; PDF/XML extraction
is still out of scope.
The accompanying read-only audit records the current queue, reciprocity,
bill-version, and mixed-generation counts in
[`post-fix-live-audit-20260906T1128Z.md`](../reports/post-fix-live-audit-20260906T1128Z.md).

The disposable PostgreSQL fixture then exercised the repaired selector directly:
the two report models rebuilt and all 21 dependent schema/union tests passed
(`PASS=23 WARN=0 ERROR=0 SKIP=0`) in 4.43 seconds. This proves the selector
and model contract, but not the live production generation; the scheduled
run and exact candidate validation remain required.

The managed source-gap lane now also invokes the bounded
`backfill_congress_committee_reports.py` probe before reconciliation. It
prioritizes currently open committee-report exceptions, records exact 200/404
fetch evidence, and remains capped by explicit limit/concurrency/timeout
settings. Its focused wrapper/probe regression slice passes `10/10`; no probe
was started while the live HTML/dbt lock was held.
The full local suite after adding the source-gap probe guard is now `428
passed, 5 skipped`; shell syntax and `git diff --check` remain clean.
The 11:37 UTC HTML retry also confirms why the old bound was insufficient: its
previous 600-second graph timed out while `int_congress_bill_text_versions`
was still reading the archive, and its client left one uniquely tagged
PostgreSQL backend behind. That orphan was cancelled selectively by its exact
application name; the current retry is fetching the next bill-text batch.

The wrapper default is now a bounded 900-second client / 840-second server
window, with the same durable cursor and fail-closed pending flag.
The latest full local verification is `429 passed, 5 skipped`; shell syntax,
Ruff, and `git diff --check` are clean after the probe and HTML timeout
changes.
The GovInfo checkpoint writer now includes
`Congress committee-report probe status`, so the new source-gap probe's outcome
is durable in the markdown artifact rather than only in the append-only run
log.

## Plan-integration verification (2026-09-06 11:45 UTC)

The 2026-09-04 agent implementation plan remains the binding acceptance
contract for this goal. I rechecked the newly added open-exception probe
prioritization after its SQL optimization: the bounded loader now joins a
distinct open-exception set once, rather than running a correlated existence
check for every GovInfo report. The focused probe/wrapper/HTML tests pass
`15/15`, Ruff and `git diff --check` pass, and the complete local suite is
`429 passed, 5 skipped`.

This is a code-and-fixture verification only. The five skips are the guarded
storage tests without an explicit disposable test DSN. No operational refresh,
candidate export, release-pointer update, or production promotion was started
by this check. The next authoritative evidence is still the scheduled
post-fix GovInfo window, followed by a fresh exact candidate export and the
09-04 A--G validation matrix; H--I remain research-ready gates afterward.

## Post-fix scheduled-window checkpoint (2026-09-06 12:40 UTC)

The first scheduled GovInfo run after the coupled report-root/edge repair
completed all four bounded source windows through `12:15:03 UTC` with zero
failed fetches. Congress resolution returned 119; the Congress bill, hearing,
and committee-report probes all returned status 0; the nested identity audit
and test also returned status 0. Its final targeted reconciliation graph
reached the 900-second bound (`targeted reconciliation status=124`) and the
wrapper failed closed, so this remains a non-certifying operational run.

Despite the bounded timeout, the coupled public report models were reached:
read-only verification found both `hrpt770-1-119` and `hrpt782-1-119` in
`bicam.committee_reports` and in
`bicam.committee_reports_associated_bills`. The current public counts are
`bills_versions=290,350`, `bills_version_links=826,630`, and 11 reciprocity
metric rows. The source-gap queue reports 23 due reprobes and 358,889 deferred
source gaps; committee-report exceptions are currently all reviewed in the
materialized relation. These counts are evidence of the post-fix generation,
not a release pass: a fresh exact candidate still must bind the same
generation and pass the complete A--G matrix.

The durable checkpoint is
[`govinfo-window-checkpoint-20260906T124040Z.md`](../reports/govinfo-window-checkpoint-20260906T124040Z.md).
The Congress supplemental timer was lock-busy at `12:22:14 UTC` and will retry
on its next scheduled interval; no manual refresh or release promotion was
performed.

## HTML timeout cleanup hardening (2026-09-06 13:56 UTC)

A read-only backend audit during the queued HTML run found one stale DBT
backend from the prior timed-out client (`bicam-html-dbt-20260906T133649Z-91966`).
The client process was already gone, so I cancelled only that exact application
name and verified it disappeared; the current run
(`bicam-html-dbt-20260906T135215Z-127958`) remained active and untouched.

The wrapper now retries exact-name cancellation after a short unwind delay and
registers an EXIT cleanup trap with explicit TERM/INT exit handlers. This
covers systemd start-timeout exits as well as the normal client timeout. Shell
syntax and the focused HTML regression tests pass (`5 passed`); no raw data,
public relation, or release pointer was changed by the cleanup check.

## Plan-integration verification (2026-09-06 14:01 UTC)

The 09-04 plan remains integrated as the active goal's acceptance contract. A
clean local verification completed with `429 passed, 5 skipped`; the five
skips are the explicitly guarded storage integration tests without
`BICAM_TEST_DATABASE_URL`. Shell syntax, Ruff, and `git diff --check` also
pass. This verifies the implementation and regression guards, but does not
certify the live database, the currently running HTML propagation, or a
corrected-release candidate.

The current operational DBT graph remains serialized under the shared lock;
no concurrent writer, candidate export, or release-pointer operation was
started by this verification. The next release-quality action is unchanged:
wait for active writers to finish, export a fresh exact post-fix candidate,
run the complete A--G validation/comparison matrix, and only then assess the
H--I research-ready gates.

## GovInfo scheduled checkpoint and orphan cleanup (2026-09-06 14:28 UTC)

The scheduled GovInfo run completed all four bounded source windows through
`14:02:32 UTC` with zero failed fetches, resolved Congress 119, and passed
the bill, hearing, committee-report, and nested-identity checks. Its
reconciliation graph rebuilt the report root (`160,040` rows), child-state
selection (`3,149,417` rows), bill versions (`290,350` rows), reconciliation
exceptions (`358,911` rows), and three reconciliation metric rows before the
bounded 900-second target expired at model 73/77. The wrapper failed closed;
the authoritative artifact is
[`govinfo-window-checkpoint-20260906T142811Z.md`](../reports/govinfo-window-checkpoint-20260906T142811Z.md).

The timed-out client left three uniquely tagged PostgreSQL backends active.
After verifying the client had exited, I cancelled only
`bicam-govinfo-dbt-20260906T140732Z-147459` and confirmed no matching backend
remained. The GovInfo wrapper now retries exact-name cancellation and cleans
up on normal exit, TERM, and INT; its focused regression slice passes
`10/10`, with shell syntax, Ruff, and `git diff --check` clean. This is
operational hardening, not release certification; HTML extraction remains
serialized and the candidate export is still pending.

The next bounded GovInfo reconciliation window is configured for 1,800 seconds
with a 1,740-second PostgreSQL statement bound, still within the one-hour
service limit. The complete local regression suite after this hardening passes
`430 passed, 5 skipped`; shell syntax, Ruff, and `git diff --check` are clean.

## HTML propagation timeout hardening (2026-09-06 14:49 UTC)

The HTML propagation client reached its prior 900-second bound while
`int_congress_bill_text_versions` was still reading the archive. After the
client exited, its uniquely tagged backend was cancelled and verified gone;
the queued lineage pass was left untouched. The next HTML cycle now uses a
bounded 1,800-second client and 1,740-second server statement timeout, with an
exact-name cancellation fallback to `pg_terminate_backend` only after the
client has exited. The combined HTML/GovInfo wrapper regression slice passes
`15/15`; this improves propagation reliability but does not yet prove the
backlog is complete.

## 09-04 plan integration refresh (2026-09-06 15:47 UTC)

The 2026-09-04 agent implementation plan remains integrated as the binding
acceptance contract for the active goal. This refresh records the current
execution handoff after the timeout hardening and bill-text query work; it
does not expand the eleven-family product scope or authorize production
promotion.

The active goal is tracked in two release milestones:

1. **Corrected release (packages A--G):** one exact candidate must bind the
   completed build, source frontiers, reconciliation/reciprocity metrics,
   contract checks, and comparison evidence. Validation and promotion remain
   fail-closed and candidate-specific.
2. **Research-ready release (packages H--I):** the corrected candidate must
   additionally carry portable source/version evidence, frozen-input
   recreation, coverage and linkage/text evaluations, and researcher-facing
   limitations/documentation. PDF/XML parsing, Congressional Record, and
   Pushover delivery remain out of scope or disabled as specified.

The package handoff is: A/B fixture and child-state work is implemented and
the 17:28 live generation now supplies complete runtime evidence; C public
grains, reciprocity unions, bill-version supplements, and source evidence are
present but must be proven from one generation; D/E/F/G validation, build
binding, CI, and atomic promotion pass in disposable fixtures; and H/I remain
downstream for source bundling, frozen-input recreation, coverage, linkage/text
evaluation, and final documentation.

The bill-text intermediate was optimized within this contract. It now reads
resolver-selected child pages directly, preserves raw-version and parent
evidence, and avoids re-expanding bill-detail staging; the split numeric/Roman
query compiles successfully with a substantially lower estimated plan cost.
The 17:28 GovInfo run now provides runtime evidence for that rewrite; HTML
backlog completion and candidate-bound release validation remain separate
gates.

The next actions are fixed and dependency-aware: let the active HTML/lineage
jobs terminate cleanly; verify exact-name backend cleanup; export and validate
one exact post-fix candidate from the completed GovInfo generation; then finish
H--I. No live checkpoint, green model build, or partial export is sufficient
to close the goal.

## GovInfo bounded-run checkpoint after plan refresh (2026-09-06 15:54 UTC)

The scheduled GovInfo lane completed all four bounded source windows through
`15:14:15 UTC` with zero failed fetches. Current Congress resolved to 119;
the bill, hearing, and committee-report counterpart probes, pre-probe refresh,
nested identity audit, and nested identity test all returned status 0. The
targeted reconciliation graph reached the new 1,800-second client bound and
failed closed with status 124. The authoritative artifact is
[`govinfo-window-checkpoint-20260906T155457Z.md`](../reports/govinfo-window-checkpoint-20260906T155457Z.md).

The wrapper's exact-name cleanup completed: after the client exited, no
matching GovInfo DBT backend remained. This confirms the timeout-hardening
path, but it is not a candidate or release pass. The HTML extractor and
fetch-lineage lane are now queued/active behind the shared lock; the next
candidate action remains a fresh serialized refresh followed by exact
candidate export and A--G validation.

## Source-gap graph optimization (2026-09-06 16:02 UTC)

The reconciliation timeout was traced to `fct_child_state_gaps` rebuilding
every large Congress child-observation relation solely to identify incomplete
generations. The model now reads the durable
`int_congress_child_response_selections` relation directly, retaining the
same logical child-family mappings, endpoint normalization, actionable queue
shape, and exclusion of paused House families. Public observation models are
unchanged; this optimization only changes the queue's evidence path.

Evidence from the disposable PostgreSQL fixture:

- the `+fct_source_gap_queue` dependency graph fell from 65 to 36 models;
- `fct_child_state_gaps` materialized two rows in 0.08 seconds;
- `fct_source_gap_queue` materialized two rows in 0.05 seconds; and
- `child_state_gaps_are_actionable` and
  `source_gap_queue_open_rows_have_action` both passed.

The focused Python wrapper/source-gap slice remains `15 passed`. A broad
fixture build was not accepted as a CI pass because it also exposed seven
unrelated pre-existing scope/table errors; those failures are retained as
evidence rather than suppressed. The next operational HTML/DBT cycle is the
runtime test for this graph reduction.

## Optimized reconciliation checkpoint (2026-09-06 16:37 UTC)

The first scheduled GovInfo graph using the source-gap reduction selected 48
models instead of 77. It rebuilt the 3,149,417-row child-selection table in
250.44 seconds and rebuilt `fct_child_state_gaps` (400,789 rows) in 5.39
seconds. The coupled committee-report root and associated-bill edge also
completed in this generation. The graph then stalled in
`int_congress_bill_title_observations` and reached the 1,800-second bound;
the authoritative checkpoint is
[`govinfo-window-checkpoint-20260906T163738Z.md`](../reports/govinfo-window-checkpoint-20260906T163738Z.md).

All four GovInfo windows, Congress 119 resolution, counterpart probes,
identity checks, and targeted tests passed before the timeout. The client
failed closed as designed. Its uniquely tagged `BufFileRead` backend required
the exact-name cancel/terminate fallback after client exit and was verified
gone; unrelated sessions were not touched.

The title observation model has now been rewritten to join selected title
pages directly to their raw parent payloads, avoiding the full bill-detail
staging view and preserving the existing output/lineage columns. It compiles
and passes its disposable lineage and semantic-key tests; the next scheduled
graph is the runtime test. The full local Python suite remains `430 passed,
5 skipped`.

## HTML NUL normalization and propagation checkpoint (2026-09-06 17:03 UTC)

The HTML extractor encountered GovInfo bill-version content containing NUL
control bytes, which PostgreSQL text fields reject. `fetch_html_text.py` now
removes only NUL characters after decoding (and defensively before insert),
while retaining the raw response hash and all other visible content. A
regression test covers the case; the focused HTML suite passes `17/17`.

The serialized retry then completed 500 GovInfo bill-version fetches
successfully, followed by 249/250 Congress bill-text fetches (one bounded
fetch error was recorded), and rebuilt the 32-model HTML propagation graph.
The durable checkpoint reset to `pending_dbt=0`; no orphan HTML DBT backend or
release pointer was left behind. The scheduled GovInfo lane has since acquired
the lock and is the next runtime test for the title-observation rewrite.

## 09-04 plan integration: optimized reconciliation run completed (2026-09-06 17:28 UTC)

The first scheduled GovInfo reconciliation run after the source-gap and
bill-title optimizations completed successfully. The authoritative checkpoint
is [`govinfo-window-checkpoint-20260906T172848Z.md`](../reports/govinfo-window-checkpoint-20260906T172848Z.md).

Evidence for the active-goal handoff:

- all four bounded GovInfo categories completed the same window through
  `17:11:14 UTC`, with zero failed fetch events;
- Congress 119 resolution, bill/hearing/committee-report counterpart probes,
  pre-probe refresh, nested identity audit, and nested identity test all
  returned status 0;
- the optimized reconciliation graph ran 48 models (three incremental, 34
  table, and 11 view models) in 409.77 seconds, with `PASS=48 WARN=0
  ERROR=0 SKIP=0`;
- the nine targeted dbt assertions completed with `PASS=9 WARN=0 ERROR=0
  SKIP=0`, including bill-version evidence, committee-report union coverage,
  and source-gap queue actionability; and
- the source-gap graph materialized 759,700 queue rows, while the title
  observation rewrite materialized 1,125,570 observations in 10.52 seconds.

This is operational runtime evidence for the corresponding 09-04 packages,
not release certification. The run was not an export or promotion, and the
current candidate still must be regenerated from this completed generation
and pass the exact A--G contract, build/frontier, comparison, and fail-closed
promotion checks. HTML extraction and fetch-lineage wrappers remain serialized
behind the shared lock; waiting is not a failed result.

The next dependency-aware action is a fresh candidate export bound to this
generation, followed by offline validation and self-comparison in a unique
candidate directory. H--I source bundling, frozen-input recreation,
coverage/linkage/text evaluation, and researcher documentation remain
downstream. No release pointer moved.

## Candidate-bound corrected-release evidence (2026-09-06 18:40 UTC)

The post-refresh candidate export and exact comparison now provide the
candidate-level evidence required by the 09-04 A--G contract. The candidate is
[`manifest.json`](../reports/exports/operational-20260906T175700Z/20260906T180847Z/manifest.json)
with database snapshot `2026-09-06T18:08:47.449428Z` and build ID
`18269a106933544456d9d0848e0ae159843aa5e121782f74f3fac08fea5e3bb5`.

- The exporter wrote 91 relations (89 core plus `bills_versions` and
  `bills_version_links`) with 32 source frontiers, a complete build status,
  portable source/dependency metadata, and 506/506 row-level checks passing.
- The exact candidate dry-run returned `valid=true`, `promoted=false` under
  contract version 3; all declared files, headers, row counts, byte sizes,
  SHA-256 hashes, supplemental scope, and provenance binding verified.
- The disk-backed comparison against the prior complete candidate is
  [`release-candidate-comparison-operational-20260906T180847Z.json`](../reports/release-candidate-comparison-operational-20260906T180847Z.json).
  It compared all 91 relations with exact inventory. Nonzero changes are
  limited to the refreshed bill/bill-version, bill-text, committee-report,
  committee-print-text, hearing-text, and nomination-position surfaces; the
  comparison retained bounded row/key diffs rather than treating count changes
  as unexplained failures.
- The mutable `reports/exports/latest` pointer remains on the previous
  accepted release (`20260905T050822Z`); no production promotion was attempted.

The read-only release-freshness check against this exact candidate also passed
at 18:43 UTC: manifest age was 0.58 hours, there were zero stale running
crawls, zero unsatisfied child fetches, zero reconciliation non-pass rows, and
zero open reconciliation exceptions. Its classified source-error count is 13
and remains explicitly reported rather than hidden.

The candidate's reconciliation metrics are all passing (3/3), while the
Congress internal reciprocity audit intentionally remains an auditable review
surface (11 relationship types, all `quality_status=review`, zero
contradictions). Those asymmetries are evidence for the source-side review
queue, not silently converted into reciprocal truth. H--I still remains open:
the candidate's source bundle is metadata-bound and excludes raw payloads,
and frozen-input recreation, coverage/linkage/text evaluation, and final
researcher documentation are not yet complete.

## HTML lane after candidate capture (2026-09-06 18:44 UTC)

The candidate snapshot is immutable, but the scheduled HTML lane continues
forward for the next generation. Its latest durable progress report records
677,845 eligible targets and 152,612 successful extractions, with
`pending_dbt=0`; a new 250-row Congress bill-text batch is active. This later
work is intentionally not folded into the already validated candidate. The
shared lock remains held by the extractor, and no candidate or release pointer
is being modified by the scheduled lane.

## Post-candidate scheduled-lane checkpoint (2026-09-06 18:45 UTC)

The next GovInfo window after candidate capture also completed successfully:
all four categories reached `18:10:42 UTC`, Congress 119 resolved, all three
counterpart probes and both nested-identity checks returned status 0, and the
targeted reconciliation/test selectors passed. Its exact artifact is
[`govinfo-window-checkpoint-20260906T182933Z.md`](../reports/govinfo-window-checkpoint-20260906T182933Z.md).

The independent HTML progress report now shows 153,112 successful current
extractions (46,124 bills, 50,999 bill versions, 334 committee prints, 19,118
committee reports, and 36,537 hearings) out of 677,845 eligible targets;
`pending_dbt=0` and another 250-row Congress bill-text fetch is active. The
lineage pass repaired zero rows; its separate freshness status remains 1 only
because the mutable `latest` manifest is intentionally older than 24 hours.
These post-candidate changes belong to a future candidate generation and do
not modify the validated 18:08:47 snapshot.

## Active-goal integration record (2026-09-06 18:48 UTC)

The 2026-09-04 agent implementation plan is now explicitly integrated into
the original active goal. Its deliverables map as follows:

| Active-goal deliverable | Plan packages | Current disposition |
| --- | --- | --- |
| Incremental propagation and child-state correctness | A--B | Implemented and exercised in disposable fixtures; operational generations now retain baseline-plus-delta evidence. |
| Public eleven-family product and honest relational grains | C | Implemented in the corrected models/contracts, including nominee, title, and vote grain fixes; candidate evidence exists, with final research-release validation still open. |
| Congress.gov reciprocity as a first-class audit | C, F, G | Implemented as an eleven-family audit and union-preserving relationship layer. Current metrics show zero contradictions but remain `review` where source asymmetry or unavailable endpoints is expected; they are not silently treated as complete reciprocity. |
| Supplemental bill versions and version links | C, H | Included in the candidate (`bills_versions`, `bills_version_links`) and validated for inventory/lineage. Portable source-row traces and frozen-input recreation remain H gates. |
| Managed source-gap queue with alerts disabled | B, F, G | Implemented, actionable, and included in release quality evidence. Pushover wiring remains intentionally disabled; classified source errors stay visible. |
| Linked HTML text extraction | B, F, H, I | The extraction/propagation lane is live and resumable, with 153,112 successful current extractions and no pending DBT work at the last checkpoint. The remaining backlog is deliberately future-generation work and cannot mutate the validated candidate. |
| Reproducible research release | D--I | Candidate-level A--G evidence is present and the prior accepted pointer was preserved. H--I remain open for distributable source evidence, frozen-input recreation, linkage/text evaluations, and researcher documentation. |

The active goal therefore has a two-gate definition of done: first produce a
complete corrected candidate that passes A--G, then produce the research-ready
release that also passes H--I. The validated candidate at
`20260906T180847Z` satisfies the recorded candidate checks but is not the
research-ready release and was not promoted. The next agent must continue from
that candidate and its immutable comparison artifacts, not restart the
pipeline or infer completion from the green operational checkpoints.

## Offline quality evaluation and CSV null semantics (2026-09-06 18:56 UTC)

The release path now declares its portable CSV encoding explicitly: UTF-8,
LF-newline, header-bearing gzip CSV with `\\N` as the SQL NULL marker. The
exporter emits that marker through PostgreSQL `COPY`, records the setting in
the manifest and provenance sidecar, and the candidate validator checks it for
new-format candidates while retaining compatibility with older candidates.
The focused exporter/promotion/source-evidence suite remains green (`28
passed`), and Ruff plus bytecode checks pass.

The new streaming evaluator
[`evaluate_research_snapshot.py`](../bicam-collection/scripts/evaluate_research_snapshot.py)
produced a representative offline report at
[`offline-candidate-evaluation-20260906T1855.json`](../reports/quality/offline-candidate-evaluation-20260906T1855.json)
and its Markdown summary. It scanned six immutable candidate relations:
428,967 bills, 394,340 bill-text rows, 290,350 GovInfo bill versions, 826,631
version links, 46,487 hearings, and 18,065 committee meetings. Bill-version
links were 814,006 linked and 12,625 without a matching GovInfo version. The
report correctly labels the prior candidate's NULL semantics as ambiguous
because it predates the new serialization field; it makes no population,
linkage-accuracy, or manual text-quality claim.

The exporter now also has an opt-in `--include-source-evidence` mode. It
selects bounded immutable raw-response versions inside the same repeatable-read
transaction, embeds the content-addressed bundle under the candidate, records
its snapshot/digest metadata in the manifest and provenance sidecar, and can
be enforced at promotion with `--require-source-evidence`. The default core
export remains unchanged and does not copy raw payloads.
The positive embedded bundle validation path is covered by the focused suite
(`32 passed`).

## Extraction-only continuation checkpoint (2026-09-06 19:08 UTC)

The original operational goal's non-extraction work remains green in the live
database: reconciliation metrics are `pass` for bills (428,966), committee
reports (20,233), and hearings (45,858), with zero open exceptions; the
Congress reciprocity audit has all 11 declared relationship families, zero
contradictions, and explicit `review` status for expected asymmetry; the source
gap queue contains 759,700 rows, including 400,812 actionable rows; and the
bill-version surfaces contain 290,350 versions and 826,631 links.

The HTML lane is healthy and resumable. Its latest progress report has 46,374
successful bill-text extractions and 51,249 successful GovInfo bill-version
extractions, with the wrapper active and a new 250-row Congress bill-text fetch
underway after the shared lock became available. Pending DBT work is zero at
the checkpoint. The remaining extraction backlog is intentionally allowed to
continue asynchronously; it does not block the completed non-text scope.
