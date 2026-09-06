# Bills public family

This pilot mirrors the published bills family with canonical, plural table
names in schema `bicam`. `bills_lobbied` is intentionally deferred: no
authoritative lobbying source or stable bill crosswalk is available in this
collection. Voteview joins are likewise deferred and are not represented by a
fabricated table or column.

The canonical bill grain and all Congress.gov child features are populated
from the reconciled source models. GovInfo BILLS package metadata and package
reference collections are included when present; fields not supplied by the
harvested source (for example page counts) remain null rather than being
invented. Internal raw IDs, source URLs, and reconciliation decisions are not
projected into these relations.
