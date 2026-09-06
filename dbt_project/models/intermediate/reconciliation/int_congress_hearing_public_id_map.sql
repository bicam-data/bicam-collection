{{ config(
    tags=['hearings', 'reconciliation', 'intermediate'],
    indexes=[
      {'columns': ['source_hearing_id'], 'unique': true},
      {'columns': ['public_hearing_id']}
    ]
) }}

/*
  Congress.gov route jacket numbers are not always publication jacket
  numbers. The format URLs normally carry the authoritative CHRG package
  identity; for example route /117/house/10 publishes
  CHRG-117hhrg44535-add10. ``-addN`` identifies a child/addendum of the base
  package, while published package suffixes such as p1, Op1, O, and v1 remain
  part of the stable root identity.
*/
with formats as (
    select
        hearings.hearing_id as source_hearing_id,
        hearings.raw_id,
        (regexp_match(
            format.value ->> 'url',
            '(CHRG-[0-9]+[hsj]hrg[0-9]+[^/.]*)',
            'i'
        ))[1] as referenced_package_id
    from {{ ref('int_hearings__congress') }} as hearings
    join {{ ref('int_raw_responses__current') }} as raw
      on raw.raw_id = hearings.raw_id
    cross join lateral {{ unnest_array("raw.response -> 'hearing'", 'formats', 'format') }}
),
package_evidence as (
    select
        source_hearing_id,
        raw_id,
        regexp_replace(min(referenced_package_id), '-add[0-9]+$', '', 'i') as package_id,
        count(distinct regexp_replace(
            referenced_package_id,
            '-add[0-9]+$',
            '',
            'i'
        ))::integer as package_candidate_count
    from formats
    where referenced_package_id is not null
    group by source_hearing_id, raw_id
),
mapped as (
    select
        hearings.hearing_id as source_hearing_id,
        hearings.raw_id,
        evidence.package_id as format_package_id,
        coalesce(
            {{ bicam_govinfo_hearing_id('evidence.package_id') }},
            hearings.hearing_id
        ) as public_hearing_id,
        coalesce(evidence.package_candidate_count, 0) as package_candidate_count,
        case
            when evidence.package_id is not null
                then 'congress_format_url_chrg_package'
            else 'congress_route_identity_fallback'
        end as identity_rule
    from {{ ref('int_hearings__congress') }} as hearings
    left join package_evidence as evidence
      on evidence.source_hearing_id = hearings.hearing_id
     and evidence.raw_id = hearings.raw_id
)

select *
from mapped
