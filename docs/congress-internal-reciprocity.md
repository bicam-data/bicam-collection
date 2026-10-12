# Congress.gov internal reciprocity

`int_congress_internal_relationship_edges` extracts the relationship edges
reported by both sides of the Congress.gov API. `int_congress_internal_relationship_union`
groups those edges by relationship type and IDs, retaining a row whenever
either side reports it and recording `left_present`/`right_present` flags.
Both downstream categories therefore consume the same canonical union rather
than dropping a relationship because one endpoint is incomplete.

The audit covers member sponsor/cosponsor ↔ bill, member sponsor/cosponsor ↔
amendment, committee ↔ bill, bill ↔ committee report, and committee ↔
nomination. One-sided rows are marked
`counterpart_unavailable` until endpoint coverage is independently verified;
they are not silently discarded or called contradictions. The aggregate rates and review status are in
`fct_congress_internal_reciprocity_metrics`.
