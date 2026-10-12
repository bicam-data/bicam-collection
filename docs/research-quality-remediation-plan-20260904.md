# BICAM research-quality remediation: agent implementation plan

Date: 2026-09-04. Status: implementation in progress; production deployment and corrected-release promotion have not been performed. The live execution tracker is [research-quality implementation status](research-quality-implementation-status-20260905.md), and its checkpoint is [the 09-04 plan integration report](../reports/research-quality-plan-integration-checkpoint-20260905T220100Z.md).

Primary evidence: [September 4 review](../reports/research-quality-review-20260904.md). Compatibility context: [public-product handoff](bicam-public-product-handoff-20260831.md). Treat older statements that the product is fully validated as superseded by the concrete failures in the review.

## Objective and completion criteria

Produce a corrected, reproducible research release that preserves legitimate source records, reconciles repeated observations, satisfies its declared relational grains, and cannot become `latest` without successful validation of that exact candidate.

There are two milestones:

1. **Corrected release:** packages A–G below pass; a complete candidate passes the release acceptance matrix and comparison audit in an isolated environment. Production promotion is a separate authorized operation.
2. **Research-ready release:** packages H–I also pass, including bundled provenance, version history, missingness documentation, and evidence-based linkage/text evaluations. Do not describe milestone 1 as completion of the full research-quality objective.

Preserve the eleven root families, 89 core table names, source identifier forms, and public column order wherever possible. Keep provenance in supplemental artifacts. Correct false key declarations through a versioned contract migration. Do not force records into an incorrect key to preserve a false compatibility claim. Lobbying/Voteview crosswalks remain deferred.

## Execution rules for every agent

- Read applicable `AGENTS.md` and the cited review before editing. The user authorized reviewing all existing changes; this plan is not blanket authorization to overwrite those changes or operate production.
- All paths below are relative to `/home/rdelano/bicam` unless they begin with `/`. `bicam-collection/` is the main Git repository; the operational lane wrappers that once lived in the sibling `scripts/` directory are now in `bicam-collection/ops/`.
- Do not run migrations, crawls, broad dbt builds, test writes, timers, or symlink promotion against the operational database/release directory during implementation. Use a dedicated disposable PostgreSQL database with dedicated credentials and a temporary export directory.
- Before each edit, inspect its existing content and record ownership with the coordinator. Stop for unexpected changes. Do not reset, mass-delete, or restore files belonging to another task.
- Preserve raw responses, response versions, fetch evidence, and the September 4 export. Corrections belong in derived models and new candidate exports, not historical artifact rewrites.
- Do not weaken tests, raise allowed error counts, classify new exceptions automatically, or discard valid rows just to make a gate pass.
- Retain the shared ingestion lock convention in operational wrappers. A lock coordinates jobs; it is not evidence that materializations share one input generation.
- Report test commands, fixtures, unresolved ambiguities, and known limits accurately. Never claim a database test passed because a Python test merely inspected SQL text or fixture labels.

## Coordination, baseline, and dependencies

One coordinator owns the integration branch and shared interfaces. Before delegation, preserve the authorized working tree as the task baseline: record HEAD, modified paths, and an explicit allowlist of source/config/test files and necessary untracked files. Include the required sibling scripts and Rotisserie revision/content changes. Do not bundle `.env`, credentials, logs containing secrets, caches, or raw datasets. A fresh worktree at HEAD alone is insufficient because much of the implementation is uncommitted.

Use isolated checkouts containing this baseline, or exclusive file ownership in the shared workspace. Do not ask agents to independently rewrite the exporter, contract, or CI workflow. Record a baseline content digest and the exact handoff revision for each integration.

| Package | Owner role | Dependencies | Principal output |
|---|---|---|---|
| A | Test isolation agent | Baseline | Safe test DSN, adversarial source fixtures |
| B | Child-state agent | A fixtures/interface | Complete baseline plus delta reconstruction |
| C | Public-grain agent | B interface; final checks after B | Correct nominees/titles/votes and contract semantics |
| D | Contract-validation agent | C contract interface | Data-level and artifact validation |
| E | Release agent | D validation interface | Candidate validation and atomic promotion |
| F | Build-evidence agent | Baseline; E manifest interface | Build generation, frontiers, snapshot-bound quality |
| G | CI/integration agent | A; final integration after B–F | Full fixture build/export/negative release tests |
| H | Provenance agent | C row identity; E–F bundle interface | Portable evidence and bill-version supplements |
| I | Research-validation agent | Fixture design can start early; final metrics after B–H | Linkage/text evaluations and researcher documentation |

Recommended sequence with three worker slots: A plus early F design and I fixture design; then B, C design, and D scaffolding; then C/D integration plus E; then F/G integration; finally H/I and the full release comparison. Parallelize only after interfaces are agreed. The coordinator owns cross-package edits and deploy-script integration.

Before coding dependent packages, record these interface decisions in a short decision log in the PR:

1. B: response-selection output retains endpoint, logical collection, page, fetch/version evidence, full/delta classification, ordering fields, and completeness status. Downstream models need more than a payload alone.
2. C: contract distinguishes unique keys, grouping keys, deterministic export ordering, required values, and foreign keys. A relation without a trustworthy unique public key must say so explicitly.
3. D/E: validation returns structured pass/fail, contract/bundle identity, check identifiers, counts and bounded examples. Validation errors and timeouts fail closed; they never become an empty success report.
4. E/F/H: manifest has an explicit schema version and release profile, build ID, contract digest, table inventory, source frontiers, and hashed references to immutable evidence artifacts. Content evidence must avoid circular hashes.

## Package A — Isolate tests and create regression inputs

**Own:** `bicam-collection/tests/test_storage.py`, test-only connection/fixture helpers, new regression fixture directories. Coordinate any shared fixture-loader changes with G.

1. Require and use `BICAM_TEST_DATABASE_URL` directly for integration tests. Do not remove `DATABASE_URL` from the process or load the project's `.env` for test DSN selection. Avoid changing application configuration precedence just to fix tests.
2. Without an explicit test DSN, skip integration tests locally with a clear reason; CI must supply the DSN and must fail if its required integration tests skip. Use a dedicated role/database, not a production credential plus a naming heuristic.
3. Add tests proving a project `.env` and a populated `DATABASE_URL` cannot activate or redirect storage integration tests.
4. Create small sanitized fixtures for the matrix below, including two distinct nominees at ordinal 1, repeated source observations, overlapping pages/windows, an amended title, and two votes for one action. Include negative controls that must remain distinct.
5. Preserve separate fixtures for a source duplicate within one authoritative payload versus the same observation repeated across fetches. These require different interpretation.

**Acceptance:** storage lifecycle tests run only in the disposable DB; absent test DSN causes no connection attempt; cleanup is scoped to fixture-owned records. The existing isolated suite remains green.

## Package B — Reconstruct child state correctly

**Own:** `bicam-collection/dbt_project/macros/resolve_child.sql`, supporting raw/intermediate models and their tests. Inventory every caller before changing the macro output. Coordinate staging model changes with C.

1. Map how full crawls, bounded runs, retries, filtered windows, and pages are recorded. Use exact fetch-to-response-version evidence where available. A version's first/last-seen timestamps do not reconstruct every observation: identical payloads can recur after intervening changes.
2. Define a logical collection independently of page offset and time filter. Normalize query parameters without changing meaningful endpoint/part identities. Keep pages attached to the collection generation that produced them.
3. Select the newest demonstrably complete baseline for each collection. A completed run alone is insufficient: confirm expected pages, terminal pagination, and fetch outcomes. Do not mix pages from unrelated full crawls and call the result complete.
4. Apply every relevant delta not covered by that baseline. An empty delta does not erase prior state. Define source-update precedence, observation-time fallback, deterministic ties, and how to detect conflicting equal-precedence observations.
5. Reconcile by entity-specific keys downstream. Full collections may legitimately replace prior membership; deltas may do so only with explicit supported removal semantics. Do not union all historical snapshots indefinitely.
6. When no complete baseline or exact historical linkage exists, mark reconstruction confidence/completeness explicitly. Preserve evidence and report the gap rather than inventing a complete state or initiating an unbounded recrawl.
7. Repair the existing child-count test's NULL comparison and fixture-presence checks.

**Acceptance:** baseline A + deltas B + C yields A/B/C; empty polls preserve it; later corrections supersede earlier observations; a complete replacement retires superseded state; an incomplete replacement does not. Repeated payloads, reordered query parameters, shifted page boundaries and replayed requests yield stable results. PostgreSQL/dbt fixtures execute the actual macro and affected models.

**Performance:** inspect bounded query plans in the isolated DB. Avoid a full raw-archive window sort for each staging caller. Materialize reusable selection evidence if required, with appropriate indexes. Any production-scale estimate must identify its assumptions.

## Package C — Correct public grains without deleting legitimate records

**Own:** `bicam-collection/contracts/bicam_public_schema.yaml`, the nominee/title/vote public models, their staging reconciliation and schema tests. Coordinate contract parser changes with D and exporter changes with E.

1. Trace repeated nominee rows to raw response versions and page/fetch evidence. The review found 256,536 identical excess public rows; this is a diagnosis target, not an instruction to delete exactly that many rows. Identical public fields alone are not proof that two source people are the same person.
2. Collapse repeated observations of the same source occurrence while preserving multiple nominees in one position, same-name people, and distinct occurrences within a source payload. Retain unresolved identity ambiguity with evidence. Do not use `(nomination_id, ordinal)` or names as a person identifier.
3. Remove the false nominee primary-key claim. Prefer retaining the current core columns with an honest grouping-key/no-unique-public-key declaration and a supplemental occurrence identity. That identity may be release-scoped if cross-release identity cannot be justified; do not present payload position or a content hash as a stable person ID. If a new core column is unavoidable, implement it as an explicit versioned schema migration with compatibility documentation.
4. Define vote grain as action-to-vote. Check a candidate key using action plus Congress/chamber/session/roll identity against real data and fixtures; handle unavailable identity components explicitly. Keep Senate rolls 183 and 184 for the audited `hr4567-108` actions, and rolls 354 and 355 for `samdt3681-117`.
5. Resolve title observations according to source/update precedence. The `hr10061-119` title with absent metadata and with IH metadata must not produce conflicting current rows under the declared key. Preserve legitimately distinct text versions; do not assume non-null metadata is always newer or correct.
6. Version the corrected contract and align indexes, dbt tests, parser assumptions, export sort order, downstream version links, and documentation. Use a total export order even for relations without a unique public key; do not advertise a sort key as a primary key.

**Acceptance:** all four audited tables satisfy their corrected semantics. Fixture negatives prove distinct nominees/votes/titles survive. Produce a before/after classification of removed, changed and retained observations with evidence, including any remaining identical rows that represent unresolved or legitimate source multiplicity.

## Package D — Make the contract executable against rows and files

**Own:** `bicam-collection/scripts/validate_bicam_product.py`, contract-validation helpers, relevant tests and generated/parameterized dbt checks. E owns `check_release_freshness.py`; expose reusable validation rather than editing it independently.

1. Validate contract structure: unique relation names, existing key columns, supported types, reference targets, and explicit null/empty-value policies. Distinguish `required_columns` as schema-presence requirements from declared non-null value requirements; specify both where needed.
2. Check actual row-level unique keys, required non-null values, foreign keys (including child-to-child and any declared composite references), and approved domain constraints. Use null-safe semantics and bounded violation examples. Do not infer that a PostgreSQL unique index disallows NULLs.
3. Do not reject all empty tables indiscriminately. Require every relation and expected quality category/entity; legitimate zero coverage must have an explicit scope rule or observed-empty evidence.
4. Implement artifact checks against the bundled, hashed contract: exact required relation set for the profile, unique filenames/relation entries, schema/contract versions, ordered CSV headers, parsed row counts, sizes and SHA-256. Reject unknown extras unless the declared supplemental profile allows them.
5. Preserve SQL NULL versus empty text semantics. Perform authoritative value validation inside the export snapshot; define and document CSV encoding. Offline validation must not silently equate quoted empty strings with SQL NULL.
6. Add scalable streaming or disk-backed verification for exported keys/references; avoid holding the largest text tables in memory. Where validation relies on a bundled database report, authenticate and bind it to the exact export rather than pretending to recheck all semantics offline.

**Acceptance:** deliberately missing/duplicate relations, wrong headers, invalid keys, orphans, required NULLs, corrupt bytes, malformed contracts, and false empty-metric success all fail. Valid nullable fields and explicitly legitimate empty collections pass. All checks have machine-readable identifiers.

## Package E — Validate candidates before promotion

**Own:** `bicam-collection/scripts/export_research_snapshot.py`, `scripts/check_release_freshness.py` under that repository, their tests. The coordinator integrates the workspace `scripts/run_research_snapshot.sh` changes supplied by this agent.

1. Split the lifecycle into candidate creation, validation and explicit promotion. The exporter must not update `latest` as a side effect of writing CSVs.
2. Keep candidate files and manifest in a unique directory. Validate that directory by exact path and digest, never by following a mutable `latest` pointer.
3. Require a complete core inventory for core promotion; a research profile must additionally contain its declared supplements/evidence. Partial `--tables` exports are marked partial and cannot promote to the complete-release channel. Disallow promotion after bypassed file checks.
4. Capture data-quality checks in the same repeatable-read snapshot as the exported data, using F's completed build evidence. Verify artifact bytes after writing, then record the passing validation result outside the content-digest cycle.
5. Update `latest` atomically only after all checks pass. At promotion, revalidate the candidate identity/digest and protect against concurrent attempts or an older candidate displacing a newer accepted release. Preserve the last good target on any error.
6. Retain failed candidates with their diagnostics or apply a documented candidate-only cleanup policy. Never remove accepted snapshots during failure cleanup.
7. The runner must propagate export, validation and promotion failures with distinct status fields. Operational reports must identify the candidate and accepted release separately.

**Acceptance:** inject failures at copy, hashing, schema validation, quality checks and promotion. In every case the prior good release remains selected. A one-table export cannot replace it. Concurrent promotion and candidate modification after validation are rejected or safely serialized. A standalone candidate can be validated without production connectivity.

## Package F — Bind a release to a completed build and source frontiers

**Own:** new build-evidence model/helper and tests. The coordinator owns shared exporter and operational-wrapper integration. Use an additive numbered migration only if persistent build bookkeeping is necessary; do not rewrite applied migrations.

1. Inventory every producer of public tables and affected dependencies, including sibling scripts. A new manifest timestamp must never stand in for a fresh public build.
2. Define a build ID tied to code/contract/configuration digests and captured input frontier. Record per-relation completion and relevant dependencies; mark the overall build complete only after required models and checks succeed.
3. Use a bounded coordinated build on frozen/captured inputs, or an isolated build schema that is accepted as a complete generation. Select the smallest approach supported by the current pipeline. Do not require every relation to rebuild unnecessarily, but inherited relations need evidence that their relevant inputs have not changed.
4. Separate source-effective timestamps, fetch observation times, successful source-window frontiers and public build completion time. Configure freshness by source/family/schedule; historical immutable records are not stale just because their source dates are old.
5. Check presence of the expected metric entities/categories/partitions from declared scope. Bind source-gap evidence and its expiry to release time, including reviewed exceptions. A historical release can be valid at its original acceptance time even when a later gap review becomes due.
6. Export these results from the same database snapshot as the public data. Later live checks may report current health, but cannot retroactively certify an earlier artifact.

**Acceptance:** a fresh export of stale public tables fails; incomplete/mixed generations fail; empty required metrics fail; an unchanged historical partition with valid frontier evidence passes. Interrupted builds cannot acquire completed status. Explain the chosen serialization/isolation strategy in the PR.

## Package G — Exercise the whole product in CI

**Own:** `bicam-collection/.github/workflows/ci.yml`, CI fixture loader orchestration and integration tests. Coordinate all shared fixture/schema changes with A–D.

1. Immediately replace the invalid dry-run names `member_legislation committee_bills` with the default full plan or valid core names. Retain rejection tests for the obsolete names.
2. Provision an isolated PostgreSQL database, run migrations, load representative fixtures, and build the public product with required ancestors and tests. Use the actual dbt graph; merely selecting a schema without building missing ancestors is insufficient.
3. Run D's schema/data validator, create a real complete export, validate it, and promote only within a temporary directory. Exercise the negative release cases, not just the happy path.
4. Ensure every core relation is exercised; fixture populations must cover the failure-prone families and explicitly document valid empty relations. Missing fixtures must not make tests pass vacuously.
5. Retain existing unit, lint and contract checks, with integration tests required after A is fixed. Upload sanitized validation/build artifacts on failure. Keep the Rotisserie dependency revision reproducible and account for required sibling-script changes in the build input.

**Acceptance:** CI passes from the frozen baseline plus integrated changes without local `.env`, downloaded production data, or an existing dbt target. Each seeded defect fails the intended check. The passing suite tests actual reconciliation SQL rather than comparing expected labels to themselves.

## Package H — Ship reproducibility and version evidence

**Own:** new supplemental provenance artifacts/models, release-bundle metadata and tests. Coordinate changes to manifests with E/F and contract entries with C.

1. Include `bills_versions` and `bills_version_links` in an explicit research bundle while preserving the core-only profile.
2. Map public rows/occurrences to source version IDs, sanitized endpoint identity, observation times, transformation rule version, and selection/merge evidence. Many-to-many lineage must remain many-to-many. Mark unresolved historical linkage rather than guessing it.
3. Include archived contract/configuration, code revision plus worktree/source-bundle digest, dependency lock and local dependency revision, dbt manifest/run/test results, input frontiers, and release-specific quality/gap reports. Use an explicit source-file allowlist; exclude credentials and unrelated local files.
4. Provide enough source evidence for the advertised reproducibility level: distributable content-addressed payloads, or a documented immutable retrieval mechanism. IDs into a private mutable database alone do not make the bundle independently reproducible. Clearly state any remaining evidence-access limitation.
5. Test representative public-row-to-source traces, version link targets, and recreation of outputs from a sanitized frozen input slice. Treat separate source-effective and observed-at time semantics as prerequisites for any as-of-history claim; do not claim full historical reconstruction until tested.

**Acceptance:** users can verify the bundle offline, identify exactly which code/contract/data frontier produced it, inspect representative source decisions, and load all bill versions without joining through an arbitrary representative package.

## Package I — Quantify research limitations and document use

**Own:** new quality evaluation fixtures/reports and researcher documentation. Coordinate actual matcher/extractor changes as separate scoped fixes if evaluation discovers them.

1. Publish coverage/missingness by family, Congress, chamber, source and important field. Show all-observed, source-eligible and source-gap denominators together, plus classified and actionable missingness. Do not call 100% eligible matching 100% population completeness.
2. Build an independently adjudicated stratified linkage corpus containing matches, nonmatches, ambiguity, historical IDs, report parts and source-only records. Run actual matching SQL. Report precision/recall with sample sizes, uncertainty, strata and selection method; assess candidate-generation misses, not only accepted pairs. No invented labels or unsupported accuracy claims.
3. Evaluate text extraction against manually inspected source documents across era/type/format. Report fetch error, source absence, extraction failure, empty extraction and success separately; include truncation/boilerplate/encoding regressions. Define achievable thresholds from evidence and record the rationale rather than choosing values that merely pass today's data.
4. Bundle a data dictionary, null semantics, corrected grains/keys, representative-version warning, source boundaries, unresolved ambiguity, release citation, migration notes and tested loading/join examples. Document existing reuse terms accurately; do not invent licensing authority. Disclose deferred external crosswalks.

**Acceptance:** reports are generated from the release inputs and bundled with it; examples execute on fixtures; automated checks verify denominator consistency. Any independent review still needed is named as an outstanding dependency, not silently counted as completed work.

## Required release acceptance matrix

| Scenario | Required result |
|---|---|
| Full A, delta B, delta C, empty delta | A/B/C retained |
| Repeated fetch/window and overlapping pages | No extra derived occurrence for the same source occurrence |
| Two same-position or same-name people | Both retained unless source identity proves repetition |
| Later correction / complete replacement | Defined current state; earlier evidence retained |
| Incomplete pagination / failed follow-up | No false complete baseline; explicit gap status |
| Same action with two roll calls | Both votes retained under correct vote grain |
| Same title with corrected metadata | Deterministic current representation; history in evidence |
| Missing fixture or NULL count after a left join | Regression test fails |
| Bad key, orphan, required NULL, wrong header | Candidate rejected |
| Missing/duplicate table or wrong contract/profile | Candidate rejected |
| Missing metric entity or fresh export of stale build | Candidate rejected |
| Partial export, corrupt file, skipped verification | Complete-release promotion rejected |
| Failure/crash/concurrent promotion | Previous accepted release preserved |
| Different database timezone/session settings | Equivalent exported semantics and deterministic serialization |
| Same frozen input and configuration, repeated build | Same canonical rows and compressed data hashes |

Generated-at metadata may differ between reproducibility runs; data-file hashes should not. Explicitly normalize encoding, timezone, formatting and export order where necessary.

## Integration, comparison, and production handoff

1. Integrate one package at a time on the preserved working-tree baseline. Run its targeted tests before dependent integrations. Keep a mapping from all eight review findings to implementation and regression tests.
2. Run the complete fixture pipeline in disposable PostgreSQL. Confirm no required integration test skipped and no validation check was bypassed.
3. Reproduce the September 4 baseline audit using its exact timestamped directory, not `latest`. Add all six previously excluded large files and every declared foreign key using bounded streaming/disk-backed checks. Distinguish manifest inventory checks from actual file hashing.
4. Prepare a corrected candidate against a frozen representative source copy, then the full frozen input dataset when available. Full-data validation requires an authorized copy or separately authorized production read/build procedure; do not infer that permission from this implementation plan.
5. Produce row/key diffs by table and Congress. Attribute nominee reductions to proven repeated observations; preserve source multiplicity and disclose uncertainty. Explain title changes, demonstrate retention of the cited votes, and account for records restored from earlier deltas. Do not force the final nominee count to equal the baseline minus 256,536.
6. Require zero unexplained contract violations, preserved legitimate records, passing complete-product validation, tested failure-safe promotion, and a reproducible candidate report. Every unresolved limitation must have an explicit disposition and must not be hidden by an aggregate pass flag.
7. Deliver the exact code/source bundle, migration list, commands and selectors, resource/time estimates, lock behavior, candidate location/digests, validation report, and rollback instructions. Production rollout approval is requested only after this result is concrete and reviewable, as required by the supplied production stop condition.
8. On authorized rollout, stage/build/validate first and promote last. Retain the prior release for rollback. A rollback switches the accepted release pointer through the same controlled mechanism; it does not delete raw history, rewrite old exports, or automatically reverse additive migrations. Verify the final pointer and digest and record the accepted build ID.

## Required agent handoff format

Each agent returns: package ID; baseline revision/content digest; changed files; behavior and interface changes; migrations or compatibility effects; tests run and exact outcomes; fixture evidence for its acceptance criteria; performance findings; unresolved questions; and the next package that can proceed. Include a concrete diff/commit where the workspace supports it. Do not mark a package complete with only a design or a plan to test later.
