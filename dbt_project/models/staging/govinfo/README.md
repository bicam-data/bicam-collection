# GovInfo committee-report identity

GovInfo `SERIALSET` package metadata carries both a catalog number
(`serialSetNumber`) and the printed report number (`documentNumber`). The
canonical committee-report identity uses `documentNumber`, report class, and
Congress. A serial-set number is never concatenated into the report number;
for example, a package describing HRPT 10 in Congress 16 resolves to
`hrpt10-1-16`, even when its catalog number is 571.

The raw package and dedicated `stg_govinfo__committeereports_serialset` rows
retain the catalog identifier for provenance. The intermediate GovInfo report
model keeps every supported CRPT/SERIALSET package-part row for a future
public union and marks one deterministic representative per canonical report
for source matching. Repeated historical packages are therefore preserved as
evidence without turning an ambiguous package set into additional matches.

Non-report document classes and collection leakage remain in raw/staging
evidence but are excluded from the report identity relation.
