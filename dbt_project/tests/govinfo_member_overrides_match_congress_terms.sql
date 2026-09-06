/* Every curated GovInfo member override must resolve to a real Congress.gov
   member term in the report's Congress (and, when supplied, the same state).
   This guards the hand-reviewed surname mappings against stale or mistyped
   Bioguide IDs while leaving unresolved evidence rows review-only. */
with override_rows as (
    select
        source_member_key::text,
        bioguide_id::text as override_bioguide_id,
        nullif(trim(state), '') as override_state,
        nullif(congress::text, '')::integer as override_congress
    from {{ ref('govinfo_member_identity_overrides') }}
), source_rows as (
    select
        members.source_member_key::text,
        reports.congress as report_congress
    from {{ ref('stg_govinfo__committeereports_members') }} as members
    join {{ ref('stg_govinfo__committeereports') }} as reports
      on reports.report_id = members.report_id
    union all
    /*
       The source-neutral member-reference bridge also accepts reviewed
       overrides for bill and hearing member arrays. Those references use
       the immutable raw-array key rather than the legacy report key, so
       include them in the same Congress-term integrity check.
    */
    select
        refs.source_member_reference_key::text as source_member_key,
        refs.congress as report_congress
    from {{ ref('int_govinfo_member_references') }} as refs
), matched_terms as (
    select distinct
        overrides.source_member_key
    from override_rows as overrides
    join source_rows
      on source_rows.source_member_key = overrides.source_member_key
    join {{ ref('stg_congress__member_terms') }} as terms
      on terms.bioguide_id = overrides.override_bioguide_id
     and terms.congress = coalesce(overrides.override_congress, source_rows.report_congress)
     and (
         overrides.override_state is null
         or upper(overrides.override_state) = upper(terms.state_code)
     )
)
select overrides.source_member_key
from override_rows as overrides
left join source_rows
  on source_rows.source_member_key = overrides.source_member_key
left join matched_terms
  on matched_terms.source_member_key = overrides.source_member_key
where source_rows.source_member_key is null
   or matched_terms.source_member_key is null
