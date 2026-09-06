{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- Every populated person-level ID must be a real Congress.gov member ID.
select
    member_reference_person_id,
    source_member_reference_key,
    bioguide_id
from {{ ref('stg_govinfo__member_references') }} as refs
where bioguide_id is not null
  and (
      bioguide_id !~ '^[A-Z][0-9]{6}$'
      or not exists (
          select 1
          from {{ ref('stg_congress__member_detail') }} as members
          where members.bioguide_id = refs.bioguide_id
      )
  )
