{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'amendments'],
    indexes=[
      {'columns': ['amendment_id'], 'unique': true},
      {'columns': ['congress']},
      {'columns': ['amendment_type', 'amendment_number', 'congress']}
    ]
) }}

with canonical_amendments as (
    select *
    from {{ ref('int_amendments__congress') }}
),
-- A detail endpoint can be observed more than once across retries or
-- overlapping refresh windows.  The canonical intermediate relation already
-- resolves the identity row, but this timestamp is sourced from the raw
-- detail staging relation.  Rank it before joining so repeated observations
-- cannot fan out the public one-row-per-amendment grain.
detail as (
    select
        amendment_id,
        updated_at,
        row_number() over (
            partition by amendment_id
            order by updated_at desc nulls last,
                     raw_response_version_id desc nulls last,
                     raw_id desc
        ) as detail_rank
    from {{ ref('stg_congress__amendment_detail') }}
),
amendment_ids as (
    select canonical_amendment_id as amendment_id
    from canonical_amendments
    union
    select legislation_id
    from {{ ref('bridge_members__legislation') }}
    where legislation_type = 'amendment'
      and legislation_id is not null
    union
    select right_id
    from {{ ref('int_congress_internal_relationship_union') }}
    where relationship_type = 'bill_amendment'
      and right_id is not null
),
notes as (
    select
        amendment_id,
        string_agg(distinct text, E'\n' order by text) as notes
    from {{ ref('stg_congress__amendment_notes') }}
    where nullif(trim(text), '') is not null
    group by amendment_id
),
amendment_counts as (
    select
        amendment_id,
        count(distinct amended_amendment_id)::integer as amendments_to_amendment_count
    from {{ ref('stg_congress__amendment_amended_amendments') }}
    where amendment_id is not null
      and amended_amendment_id is not null
    group by amendment_id
)

select
    amendment_ids.amendment_id,
    coalesce(
        amendments.amendment_type,
        regexp_replace(amendment_ids.amendment_id, '[0-9].*$', '')
    ) as amendment_type,
    coalesce(
        amendments.amendment_number::integer,
        substring(amendment_ids.amendment_id from '^[a-z]+([0-9]+)-')::integer
    ) as amendment_number,
    coalesce(
        amendments.congress,
        split_part(amendment_ids.amendment_id, '-', 2)::integer
    ) as congress,
    amendments.chamber,
    amendments.purpose,
    amendments.description,
    amendments.proposed_at,
    amendments.submitted_at,
    amendments.is_bill_amendment,
    amendments.is_treaty_amendment,
    amendments.is_amendment_amendment,
    notes.notes,
    coalesce(amendments.actions_count, 0) as actions_count,
    coalesce(amendments.cosponsors_count, 0) as cosponsors_count,
    coalesce(amendment_counts.amendments_to_amendment_count, 0) as amendments_to_amendment_count,
    detail.updated_at
from amendment_ids
left join canonical_amendments as amendments
  on amendments.canonical_amendment_id = amendment_ids.amendment_id
left join detail
  on detail.amendment_id = amendment_ids.amendment_id
 and detail.detail_rank = 1
left join notes
  on notes.amendment_id = amendment_ids.amendment_id
left join amendment_counts
  on amendment_counts.amendment_id = amendment_ids.amendment_id
