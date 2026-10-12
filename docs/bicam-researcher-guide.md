# BICAM researcher guide

This guide describes the public BICAM product as it is intended to be loaded
and queried. The public PostgreSQL schema is `bicam`; source lineage, raw
payloads, reconciliation evidence, and operational queues are supplemental
and are not required for ordinary research queries.

## The eleven root tables

The root relations are always plural:

```text
bicam.bills
bicam.amendments
bicam.committees
bicam.committee_meetings
bicam.committee_prints
bicam.committee_reports
bicam.congresses
bicam.hearings
bicam.members
bicam.nominations
bicam.treaties
```

Child relations use the same plural prefix, such as
`bicam.bills_actions`, `bicam.bills_cosponsors`,
`bicam.committee_reports_associated_bills`, and
`bicam.nominations_nominees`. The executable public contract is the authority
for exact columns, order, types, and declared grains.

## Canonical records and source unions

The public root and relationship tables are one current canonical union where
Congress.gov and GovInfo provide overlapping evidence. A source-only record is
retained when it is inside the declared source scope; a source gap or an
ambiguous identity is not silently converted into a fabricated match.

Congress.gov is the primary source for the eleven families. GovInfo adds
document/package metadata and historical coverage for supported collections,
including bills, committee prints, committee reports, hearings, directories,
and treaty documents. The bill overlap percentage must be interpreted with
the GovInfo BILLS historical coverage boundary rather than treated as a
missing-data rate.

## Common identifiers

The public identifiers remain stable source-shaped values. Examples include
`hr6322-116` for a bill, `samdt1542-118` for an amendment, `hhah00` for a
committee, uppercase Bioguide IDs for members, and `PN607-00-102` for a
nomination part. Bill-number normalization converts a Unicode `½` suffix to
`.5`.

Do not infer identity from display titles. Join on the published identifier
columns declared by the contract. A child relation may have a grouping key
rather than a unique key when the source legitimately publishes multiple
renditions or observations for the same semantic object.

## Useful starting queries

List a bill with its current actions and committee referrals:

```sql
select
    b.bill_id,
    b.bill_type,
    b.bill_number,
    b.congress,
    a.action_date,
    a.text as action_text,
    c.committee_id
from bicam.bills as b
left join bicam.bills_actions as a using (bill_id)
left join bicam.bills_committees as c using (bill_id)
where b.bill_id = 'hr6322-116'
order by a.action_date nulls last, a.action_id, c.committee_id;
```

Find bills associated with a member through either sponsorship direction:

```sql
select distinct
    b.bill_id,
    b.bill_type,
    b.bill_number,
    b.congress
from bicam.bills as b
join bicam.bills_sponsors as s using (bill_id)
where s.bioguide_id = 'A000055'
union
select distinct
    b.bill_id,
    b.bill_type,
    b.bill_number,
    b.congress
from bicam.bills as b
join bicam.bills_cosponsors as s using (bill_id)
where s.bioguide_id = 'A000055';
```

Inspect the supplemental bill-version surface without choosing an arbitrary
representative package:

```sql
select
    v.bill_id,
    v.version_code,
    v.issued_at,
    v.last_modified,
    v.formatted_text,
    v.raw_text,
    v.govinfo_package_id,
    v.is_representative_package
from bicam.bills_versions as v
where v.bill_id = 'hr6322-116'
order by v.issued_at nulls last, v.version_code, v.govinfo_package_id;
```

To see which Congress.gov title, text, or action rows were linked to those
versions, query the separate source-row link surface:

```sql
select
    source_table,
    source_row_key,
    source_version_code,
    source_version_name,
    govinfo_package_id,
    govinfo_version_code,
    link_status,
    link_rule,
    link_confidence
from bicam.bills_version_links
where bill_id = 'hr6322-116'
order by source_table, source_row_key;
```

Read extracted HTML text only when the corresponding rendition link exists:

```sql
select
    bill_id,
    type as text_type,
    formatted_text,
    raw_text
from bicam.bills_texts
where bill_id = 'hr6322-116'
order by date nulls last, type;
```

Committee-meeting/hearing links are intentionally not one-to-one. A meeting
may have one or more hearings, and a hearing may exist without a committee
meeting. Joint hearing packages use `chamber = 'joint'`; they are not copied
into House and Senate rows.

## Reciprocity and source gaps

Congress.gov internal reciprocity is an audit surface, not a filter that
removes one-sided source evidence. The internal audit relations classify each
direction as reciprocal, source-asymmetric, counterpart-unavailable, or
unclassified. The canonical relationship union keeps valid edges from both
directions.

The source-gap queue is operational evidence for missing, failed, incomplete,
or source-rejected fetches. It is not a substitute for a public row and should
be consulted when interpreting coverage denominators. Queue status is
expiry-aware; Pushover support exists as an opt-in adapter but is disabled
until credentials and an explicit enablement decision are supplied.

## Updates and limitations

Raw responses are retained as immutable versions and public relations are
rebuilt by scheduled refreshes. Snapshot reports and release manifests under
`reports/` identify the source frontiers and validation state used for a
candidate. HTML extraction is bounded to supported HTML renditions and
GovInfo `/htm` resources. PDF/XML parsing is intentionally out of scope.

For reproducible research, record the BICAM release/build identifier with a
query result and avoid assuming that a current table is an as-of historical
snapshot unless the release evidence explicitly says so.

Release CSV artifacts are gzip-compressed UTF-8 CSV with a header and LF
record separators. For candidates declaring `serialization.csv_null`, the
unquoted marker `\N` is SQL NULL; a quoted `"\N"` is literal text. Older
BICAM candidates without that declaration use PostgreSQL COPY CSV defaults:
an unquoted empty field is NULL, and `""` is an empty string. Quoted fields
can contain embedded line breaks. A reader that discards quoting before
interpreting NULL markers can lose these distinctions.

`raw_text` contains extracted content; `formatted_text` contains its source
rendition URL. Count nonblank `raw_text` values when measuring available text.
The existence of a rendition URL alone does not mean extraction succeeded.

Run an offline evaluation of a candidate with:

```bash
cd bicam-collection
uv run python scripts/evaluate_research_snapshot.py \
  --manifest /path/to/snapshot/manifest.json \
  --output /path/to/reports/candidate-quality.json
```

The evaluator verifies selected file hashes, headers, and row counts while
measuring field missingness, available text, and version-link outcomes. Reports
must be outside the candidate directory; existing reports are preserved.
`--table` and `--max-rows-per-table` produce explicitly partial evaluations.
Version-link outcomes describe the implemented rules, not measured precision
or recall; those require independent adjudication.

## Portable source evidence

Research-ready candidates can include a bounded content-addressed source
evidence bundle. It contains immutable raw-response payloads, sanitized
endpoint identities, source/version IDs, observation timestamps, and a
`manifest.sha256` verifier. Endpoint and selection-prefix metadata redact
credential query values and remove URL user information. Payloads themselves
are preserved, not recursively sanitized; inspect them for echoed credentials
before distributing a source bundle. Generate a
bounded bundle explicitly (for example, 100 bill observations) with:

```bash
cd bicam-collection
uv run python scripts/export_source_evidence.py \
  --source congress --category bills --limit 100 \
  --output-dir ../reports/source-evidence
```

The bundle is evidence for the selected observations, not a claim that the
entire raw archive has been packaged. Its selection filters and immutable
payload hashes must be retained with the candidate manifest.

Verify a bundle without database access:

```bash
uv run python scripts/verify_source_evidence.py \
  ../reports/source-evidence-ci-20260906T0820Z/20260906T081731Z
```

To bind a bounded source bundle to the same repeatable-read candidate
snapshot, add `--include-source-evidence` to the research snapshot exporter.
Promotion can require that embedded evidence with
`--require-source-evidence`; the ordinary core profile remains provenance-
light and does not package raw payloads by default.

Verification checks exact file inventory (including rejection of undeclared
directories and symlinks), payload hashes, consistent observation references,
unique raw-version IDs, timezone-aware observation intervals, and declared
selection/count metadata. Embedded evidence must match the candidate's
snapshot, selection, and both counts. Required evidence cannot be empty.
The checksum verifies integrity relative to the manifest; it is not a signature
authenticating the publisher. Retain the release's trusted manifest digest.

These are **subset integrity checks, not a reproducible-build certification**.
One observation entry represents a distinct raw payload version, with its
first/last observed times; it is not a complete fetch-event history or a
source-effective validity interval. A latest-observed sample captured with a
release does not necessarily contain that release's actual model inputs.
Public-row-to-source mappings, archived transformation inputs/configuration,
and a tested frozen-input rebuild are still needed to establish reproducibility.

### Bill-version publication replay

A small synthetic replay now exercises the actual bill-version publication
SQL twice in a dedicated empty database. It archives model inputs, source
traces, dbt build evidence, and identical output hashes. See the
[replay checkpoint](../reports/research-quality-bill-version-replay-20260907.md)
and its reconstruction instructions. This is a publication-layer diagnostic,
not a full source-to-public rebuild or an independently reviewed accuracy
study. The two defects it originally exposed are now
[fixed in source and tested](../reports/research-quality-bill-version-fixes-20260908.md):
legacy `v1rh` text URLs resolve to `RH`, and rendition metadata uses the bridge's
exact raw version. Missing or inconsistent raw-version references retain the
package with NULL rendition fields. These changes have not been rebuilt into
the live database by this implementation work. Extracted text remains the
latest successful extraction for that pinned URL, not historical/as-of text.

A subsequent [raw-product replay checkpoint](../reports/research-quality-five-workstreams-20260908.md)
builds all 91 relations twice from frozen synthetic raw JSON, validates the
resulting candidate, and verifies bill-row traces against exported CSVs.
Only 39 relations are populated in that small fixture: it is not a substitute
for real-data release validation. A separate [independent review packet](research-review-instructions-20260908.md)
samples 24 real exported bill-text occurrences, including rows without any
generated version link. Its 48 review tasks remain unadjudicated.
