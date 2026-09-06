{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'hearings'],
    indexes=[
      {'columns': ['hearing_id', 'bill_id'], 'unique': true},
      {'columns': ['bill_id']}
    ]
) }}

with reference_rows as (
    select
        granules.hearing_id,
        reference.value as reference_json
    from {{ ref('int_govinfo_hearing_granules') }} as granules
    cross join lateral {{ unnest_array('granules.granule_json', 'references', 'reference') }}
    where upper(coalesce({{ json_text('reference.value', 'collectionCode') }}, '')) = 'BILLS'
),
contents as (
    select
        reference_rows.hearing_id,
        content.value as content_json
    from reference_rows
    cross join lateral {{ unnest_array('reference_rows.reference_json', 'contents', 'content') }}
),
identified as (
    select
        hearing_id,
        lower({{ json_text('content_json', 'type') }})
            || {{ normalize_congress_bill_number_text(json_text('content_json', 'number')) }}
            || '-'
            || {{ json_int('content_json', 'congress') }}::text as bill_id
    from contents
)

select distinct identified.hearing_id, identified.bill_id
from identified
join {{ ref('hearings') }} as hearings using (hearing_id)
join {{ ref('bills') }} as bills using (bill_id)
where identified.hearing_id is not null
  and identified.bill_id is not null
