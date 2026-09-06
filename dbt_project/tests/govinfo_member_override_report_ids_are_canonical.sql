/* Curated report-member override keys must use the public committee-report
   component order: <type><number>-<part>-<congress>. The package segment is
   independent evidence for the Congress in component overrides. */
with member_override_failures as (
    select source_member_key::text as override_key
    from {{ ref('govinfo_member_identity_overrides') }}
    where split_part(source_member_key::text, '|', 1) ~ '^(h|s)rpt[0-9]+-[0-9]+-[0-9]+$'
      and split_part(split_part(source_member_key::text, '|', 1), '-', 3)::integer
          <> congress::integer
), component_override_failures as (
    select component_key::text as override_key
    from {{ ref('govinfo_member_component_identity_overrides') }}
    where split_part(component_key::text, '|', 1) ~ '^(h|s)rpt[0-9]+-[0-9]+-[0-9]+$'
      and split_part(split_part(component_key::text, '|', 1), '-', 3)::integer
          <> substring(split_part(component_key::text, '|', 2) from '^CRPT-([0-9]+)')::integer
)
select override_key from member_override_failures
union all
select override_key from component_override_failures
