{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committee_prints'],
    indexes=[
      {'columns': ['print_id'], 'unique': true},
      {'columns': ['congress', 'chamber']},
      {'columns': ['print_jacketnumber', 'congress']}
    ]
) }}

with source_candidates as (
    select
        coalesce(public_ids.public_print_id, candidates.canonical_print_id) as print_id,
        coalesce(candidates.congress_print_jacketnumber, candidates.govinfo_print_jacketnumber) as print_jacketnumber,
        coalesce(candidates.congress_congress, candidates.govinfo_congress) as congress,
        coalesce(candidates.congress_chamber, candidates.govinfo_chamber) as chamber,
        coalesce(candidates.congress_title, candidates.govinfo_title) as title,
        congress.citation,
        govinfo.session as govinfo_session,
        govinfo.pages as govinfo_pages,
        congress.source_updated_at as congress_source_updated_at,
        govinfo.source_updated_at as govinfo_source_updated_at
    from {{ ref('int_committee_print_candidates') }} as candidates
    left join {{ ref('int_committee_print_public_id_map') }} as public_ids
      on public_ids.source_print_id = candidates.congress_print_id
    left join {{ ref('int_committee_prints__congress') }} as congress
      on congress.canonical_print_id = candidates.congress_print_id
    left join {{ ref('int_committee_prints__govinfo') }} as govinfo
      on govinfo.canonical_print_id = candidates.govinfo_print_id
     and govinfo.is_representative_package
),
candidates as (
    select
        *,
        row_number() over (
            partition by print_id
            order by (congress_source_updated_at is not null) desc,
                     congress_source_updated_at desc nulls last,
                     govinfo_source_updated_at desc nulls last,
                     title desc nulls last
        ) as _rn
    from source_candidates
)
select
    print_id,
    print_jacketnumber::text as print_jacketnumber,
    congress::integer as congress,
    govinfo_session::integer as session,
    chamber,
    title,
    govinfo_pages::integer as pages,
    print_jacketnumber::text as print_number,
    citation,
    coalesce(congress_source_updated_at, govinfo_source_updated_at)::timestamptz as updated_at
from candidates
where _rn = 1
  and print_id is not null
  and print_jacketnumber is not null
  and congress is not null
