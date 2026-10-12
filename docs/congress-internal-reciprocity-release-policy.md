# Congress.gov internal reciprocity release policy

The internal reciprocity tables are an audit of relationships exposed by two
Congress.gov endpoint families (for example member → bill and bill → member).
They are not assumed to be bijections.  The public union retains an edge when
either side reports it, while the audit records which directions supplied the
evidence.

`fct_congress_internal_reciprocity_metrics.quality_status` is deliberately
diagnostic: expected source asymmetry or an unavailable counterpart keeps a
family in `review` so the operational report remains visible.

Release acceptance is separate and explicit in
`release_acceptance_status`:

- `accepted_with_source_boundaries` — at least one observed edge, every edge
  is classifiable, and no contradiction was found. One-sided relationships and
  endpoint coverage gaps remain documented evidence and do not remove the
  union edge.
- `reject_no_evidence` — the declared family has no observed edges.
- `reject_contradiction` — source evidence contains a contradiction.
- `reject_unclassified` — a relationship could not be classified.

The dbt test
`congress_internal_reciprocity_release_acceptance_is_fail_closed` enforces
this contract. Export and promotion code should reject only the `reject_*`
statuses; it should not infer acceptance by treating diagnostic `review` as a
failure.
