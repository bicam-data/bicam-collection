# Hearing Linkage Strategy

## Purpose

This note defines the canonical approach for linking GovInfo hearings to Congress hearings and grouping related hearing events into hearing sets.

## Core Rules

- `hearing_id` is the event-level identifier.
- `hearing_set_id` is the broader set-level identifier.
- `hearing_id` and `hearing_set_id` are separate on purpose. One identifies a specific hearing event; the other groups related events.
- Chamber is part of identity. Hearing sets should not span chambers by default.
- We will not use GovInfo `related` relationships.
- We will use a boolean `is_event_matched` instead of a separate event-resolution type.

## Canonical IDs

### `hearing_id`

Use the existing Congress-style event identifier:

- format: `{first character of chamber}hrg{jacket_number}-{congress}`
- examples:
  - `hhrg62790-119`
  - `shrg37721-116`

This remains jacket-based and congress-based. It should not be changed to a year-based identifier.

### `hearing_set_id`

`hearing_set_id` should be chamber-bounded and should be able to span multiple Congresses.

Preferred format when `ILS System ID` exists:

- `{first character of chamber}hrg{ils_system_id}`
- examples:
  - `hhrg001234567`
  - `shrg000377210`

Fallback format when `ILS System ID` is missing:

- use a synthetic namespace so synthetic sets cannot be confused with ILS-backed sets
- recommended format: `{first character of chamber}hrgs{synthetic_id}`
- examples:
  - `hhrgs000001`
  - `shrgs000842`

The synthetic ID should be deterministic from the grouping logic, not assigned randomly.

## Matching Model

### Event Match

Use `is_event_matched boolean`.

`is_event_matched = true` when:

- GovInfo package parses to an exact `congress + chamber + jacket_number` match in Congress data

`is_event_matched = false` otherwise.

This is intentionally strict.

### Set Match

Set grouping is allowed to be broader than event matching. It should be based on ordered rule classes, not arbitrary point scores.

## Set Resolution Types

Use one of these values for `resolution_type` on set membership:

- `ils_exact`
- `year_title_committee`
- `year_title`
- `core_title_committee_window`
- `core_title_window`
- `singleton`

## Set Confidence

Map confidence directly from the rule used:

- `ils_exact` -> `high`
- `year_title_committee` -> `high`
- `year_title` -> `medium`
- `core_title_committee_window` -> `medium`
- `core_title_window` -> `low`
- `singleton` -> `none`

## Rule Ladder

Apply these rules in order:

1. `ils_exact`
   - same chamber
   - same non-null `ILS System ID`

2. `year_title_committee`
   - same chamber
   - same normalized year-bearing title
   - same committee when committee data exists on both sides

3. `year_title`
   - same chamber
   - same normalized year-bearing title
   - committee missing on one or both sides

4. `core_title_committee_window`
   - same chamber
   - same normalized core title
   - same committee
   - hearing dates within the chosen window

5. `core_title_window`
   - same chamber
   - same normalized core title
   - hearing dates within the chosen window
   - use only for non-generic titles

6. `singleton`
   - everything else

## Hard Blocks

Do not assign the same `hearing_set_id` when:

- chambers differ
- both `ILS System ID` values are non-null and different
- the only shared signal is a generic title such as `PENDING LEGISLATION`, `MEMBER DAY`, `MARKUP OF VARIOUS MEASURES`, or `VARIOUS BILLS AND RESOLUTIONS`

## Practical Implications

- `hearing_id` is the reliable join key for event-level links.
- `hearing_set_id` is the reliable join key for grouping related hearing events, including cases that span multiple Congresses.
- Congress remains a normal column for filtering and analysis, but it should not be forced into `hearing_set_id`.
- Titles should inform grouping logic, but title text should not appear in canonical identifiers.

## Implementation Shape

This linkage system should be implemented with an internal bridge layer, not by exposing every matching artifact to users.

Recommended internal models:

- `int_congress__hearings`
  - enriched Congress hearing model
  - keeps the existing `hearing_id`
  - adds fields needed for linkage, such as title, dates, committees, and any normalized matching helpers

- `bridge_govinfo__hearing_events`
  - package-to-event bridge
  - links GovInfo packages to Congress `hearing_id`
  - includes `is_event_matched`

- `bridge_hearings__sets`
  - internal-only set bridge
  - maps both Congress `hearing_id` values and GovInfo package IDs to canonical `hearing_set_id`
  - stores `resolution_type` and `confidence`
  - should be treated as implementation detail, not a user-facing final model

User-facing hearing models can join through these bridges, but the bridge layer itself does not need to be exposed directly.
