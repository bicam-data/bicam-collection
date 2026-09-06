/*
   Every curated GovInfo bill-member override must be corroborated either by
   the corresponding Congress.gov bill sponsor/cosponsor relationship or by
   a unique Congress.gov member term whose Congress, state, and name context
   agree. The second path is necessary when the two APIs expose a documented
   one-sided relationship; it is deliberately unique and never fuzzy-only.
   Committee-report and hearing overrides use different evidence and are
   intentionally excluded.
*/
with bill_overrides as (
    select
        bridge.source_member_reference_key,
        bridge.package_id,
        bridge.linkable_bioguide_id,
        bridge.parsed_name,
        bridge.state
    from {{ ref('bridge_govinfo__member_references') }} as bridge
    where bridge.category = 'govinfo-bills'
      and bridge.override_bioguide_id is not null
), bill_keys as (
    select distinct
        overrides.source_member_reference_key,
        overrides.linkable_bioguide_id,
        overrides.parsed_name,
        overrides.state,
        bills.canonical_bill_id,
        split_part(bills.canonical_bill_id, '-', 2)::integer as bill_congress
    from bill_overrides as overrides
    join {{ ref('int_bills__govinfo') }} as bills
      on bills.source_record_id = 'govinfo:' || overrides.package_id
     and bills.canonical_bill_id is not null
), congress_edges as (
    select distinct right_id as bill_id, left_id as bioguide_id
    from {{ ref('int_congress_internal_relationship_edges') }}
    where relationship_type in ('member_bill_sponsor', 'member_bill_cosponsor')
), contextual_matches as (
    select distinct
        bill_keys.source_member_reference_key,
        terms.bioguide_id
    from bill_keys
    join {{ ref('stg_congress__member_terms') }} as terms
      on terms.congress = bill_keys.bill_congress
     and (
         bill_keys.state is null
         or upper(trim(terms.state_code)) = upper(trim(bill_keys.state))
     )
    join {{ ref('stg_congress__member_detail') }} as members
      on members.bioguide_id = terms.bioguide_id
    where regexp_replace(
          lower(coalesce(members.direct_order_name, members.inverted_order_name, members.normalized_name)),
          '[^a-z0-9]',
          '',
          'g'
      ) like '%' || regexp_replace(
          lower(
              regexp_replace(
                  regexp_replace(
                      regexp_replace(
                          bill_keys.parsed_name,
                          '^(The Honorable|The Hon|Honorable|Hon|Mr|Ms|Mrs|Miss|Dr|Lt)[.]?[[:space:]]+',
                          '',
                          'i'
                      ),
                      '[[:space:]]+of[[:space:]].*$',
                      '',
                      'i'
                  ),
                  '[^a-z0-9 -]',
                  '',
                  'g'
              )
          ),
          '[^a-z0-9]',
          '',
          'g'
      ) || '%'
), contextual_counts as (
    select
        source_member_reference_key,
        count(*) as candidate_count
    from contextual_matches
    group by source_member_reference_key
)
select
    bill_keys.source_member_reference_key,
    bill_keys.canonical_bill_id,
    bill_keys.linkable_bioguide_id
from bill_keys
left join congress_edges
 on congress_edges.bill_id = bill_keys.canonical_bill_id
 and congress_edges.bioguide_id = bill_keys.linkable_bioguide_id
left join contextual_matches
  on contextual_matches.source_member_reference_key
      = bill_keys.source_member_reference_key
 and contextual_matches.bioguide_id = bill_keys.linkable_bioguide_id
left join contextual_counts
  on contextual_counts.source_member_reference_key
      = bill_keys.source_member_reference_key
where congress_edges.bill_id is null
  and not (
      contextual_counts.candidate_count = 1
      and contextual_matches.bioguide_id is not null
  )
