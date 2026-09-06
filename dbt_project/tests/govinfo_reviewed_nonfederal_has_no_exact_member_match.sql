/* A reviewed non-federal disposition is only safe when the reference does
   not exactly identify a Congress.gov member in the same state.  This is a
   deliberately conservative guard: a hit is sent back for manual review,
   never auto-linked by this test. */
with reviewed as (
    select distinct
        source_member_reference_key,
        parsed_name,
        state,
        regexp_replace(
            regexp_replace(
                lower(parsed_name),
                '^(the |honorable |hon\\.? |mr\\.? |mrs\\.? |ms\\.? |dr\\.? |lt\\.? )',
                '',
                'i'
            ),
            '\\s+of\\s+.*$',
            ''
        ) as name_only
    from {{ ref('bridge_govinfo__member_references') }}
    where review_disposition = 'reviewed_non_federal_participant'
), members as (
    select distinct
        bioguide_id,
        state,
        regexp_replace(lower(normalized_name), '[^a-z0-9 ]', '', 'g') as member_name
    from {{ ref('stg_congress__member_detail') }}
    where bioguide_id is not null
      and state is not null
)
select
    reviewed.source_member_reference_key,
    reviewed.parsed_name,
    members.bioguide_id
from reviewed
join members
  on upper(trim(reviewed.state)) = upper(trim(members.state))
 and regexp_replace(reviewed.name_only, '[^a-z0-9 ]', '', 'g') = members.member_name
