{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'treaties'],
    indexes=[
      {'columns': ['treaty_id'], 'unique': true},
      {'columns': ['congress_received', 'treaty_number']}
    ]
) }}

/*
  Preserve the published BICAM grain: one treaty number in its receiving
  Congress. Congress.gov addresses multipart treaties through separate detail
  URLs, but those parts are source representations of one canonical treaty.
  Child tables union their evidence under this base identity.
*/
with ranked as (
    select
        lower('td' || congress_received::text || '-' || treaty_number::text) as treaty_id,
        treaty_number::integer as treaty_number,
        nullif(suffix, '') as suffix,
        congress_received::integer as congress_received,
        congress_considered::integer as congress_considered,
        topic,
        transmitted_at::date as transmitted_at,
        in_force_at::date as in_force_at,
        resolution_text,
        parts_count::integer as parts_count,
        actions_count::integer as actions_count,
        old_number,
        old_number_display_name,
        updated_at::timestamptz as updated_at,
        row_number() over (
            partition by congress_received, treaty_number
            order by
                (nullif(suffix, '') is null) desc,
                suffix nulls first,
                updated_at desc nulls last,
                treaty_part_id
        ) as _rank
    from {{ ref('stg_congress__treaty_detail') }}
    where treaty_part_id is not null
      and congress_received is not null
      and treaty_number is not null
), govinfo as (
    select
        treaty_id,
        session,
        pages
    from {{ ref('stg_govinfo__treaty_documents_packages') }}
)

select
    treaty_id,
    treaty_number,
    suffix,
    congress_received,
    govinfo.session::integer as congress_received_session,
    congress_considered,
    topic,
    transmitted_at,
    in_force_at,
    resolution_text,
    null::text as summary,
    govinfo.pages::integer as pages,
    parts_count,
    actions_count,
    old_number,
    old_number_display_name,
    updated_at
from ranked
left join govinfo using (treaty_id)
where _rank = 1
