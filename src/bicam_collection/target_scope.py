"""Authoritative Congress.gov scope for the production research dataset.

Other endpoint specs remain available for explicitly requested historical or
future work, but unattended defaults and quality gates must stay within this
set until the scope is deliberately expanded.
"""

TARGET_CONGRESS_CATEGORIES: tuple[str, ...] = (
    "bills",
    "amendments",
    "members",
    "congresses",
    "hearings",
    "committees",
    "committee-meetings",
    "committee-prints",
    "committee-reports",
    "treaties",
    "nominations",
)

# These are the cross-source entity contracts currently in production.  Other
# reconciliation models may exist for experiments, but they are not part of
# the selected unattended quality gate.
TARGET_RECONCILIATION_ENTITIES: tuple[str, ...] = (
    "bill",
    "committee_report",
    "hearing",
)
