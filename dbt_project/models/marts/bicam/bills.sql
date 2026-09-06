{{ config(
    schema='bicam',
    materialized='table',
    tags=['bills', 'public'],
    indexes=[
      {'columns': ['bill_id'], 'unique': true},
      {'columns': ['congress', 'bill_type', 'bill_number']},
      {'columns': ['introduced_at']}
    ]
) }}

with source_rows as (
    select
        canonical_bill_id as bill_id, bill_type, bill_number::text, congress,
        title, origin_chamber, introduced_at, source_updated_at as updated_at,
        2 as source_priority
    from {{ ref('int_bills__congress') }}
    union all
    select
        canonical_bill_id, bill_type, bill_number::text, congress, title,
        origin_chamber, null::date, source_updated_at, 1
    from {{ ref('int_bills__govinfo') }}
),
selected as (
    select *, row_number() over (
        partition by bill_id order by source_priority desc,
        (title is not null) desc, updated_at desc nulls last
    ) as _rn
    from source_rows
),
details as (
    select distinct on (bill_id)
        bill_id, title as congress_title, origin_chamber as congress_origin_chamber,
        introduced_at as congress_introduced_at, policy_area,
        constitutional_authority_statement, is_law,
        actions_count, amendments_count, committees_count, cosponsors_count,
        summaries_count, subjects_count, titles_count, texts_count,
        updated_text_at
    from {{ ref('stg_congress__bill_detail') }}
    order by bill_id, updated_at desc nulls last, raw_id desc
),
packages as (
    select distinct on (bill_type || bill_number::text || '-' || congress::text)
        bill_type || bill_number::text || '-' || congress::text as bill_id,
        upper(bill_version) as version_code,
        {{ normalize_chamber('current_chamber') }} as current_chamber,
        nullif(raw.response->>'pages', '')::integer as pages,
        nullif(raw.response->>'isAppropriation', '')::boolean as is_appropriation,
        nullif(raw.response->>'isPrivate', '')::boolean as is_private,
        last_modified
    from {{ ref('stg_govinfo__bills_packages') }} as package
    join {{ ref('int_raw_responses__current') }} as raw
      on raw.raw_id = package.raw_id
    order by bill_type || bill_number::text || '-' || congress::text,
        last_modified desc nulls last, package_id desc
)
select
    selected.bill_id,
    bill_type,
    bill_number::numeric as bill_number,
    congress,
    coalesce(details.congress_title, selected.title) as title,
    coalesce(details.congress_origin_chamber, selected.origin_chamber) as origin_chamber,
    coalesce(packages.current_chamber, details.congress_origin_chamber, selected.origin_chamber) as current_chamber,
    packages.version_code,
    coalesce(details.congress_introduced_at, selected.introduced_at)::timestamptz as introduced_at,
    details.constitutional_authority_statement,
    details.is_law,
    packages.is_appropriation,
    packages.is_private,
    details.policy_area,
    packages.pages,
    details.actions_count,
    details.amendments_count,
    details.committees_count,
    details.cosponsors_count,
    details.summaries_count,
    details.subjects_count,
    details.titles_count,
    details.texts_count,
    greatest(selected.updated_at, details.updated_text_at, packages.last_modified) as updated_at
from selected
left join details using (bill_id)
left join packages using (bill_id)
where _rn = 1
