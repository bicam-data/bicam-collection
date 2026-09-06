{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'hearings'], indexes=[{'columns': ['hearing_id', 'hearing_date'], 'unique': true}, {'columns': ['hearing_date']}]) }}

with congress_rows as (
    select public_ids.public_hearing_id as hearing_id, dates.hearing_date::date as hearing_date
    from {{ ref('stg_congress__hearing_dates') }} as dates
    join {{ ref('int_congress_hearing_public_id_map') }} as public_ids
      on public_ids.source_hearing_id = dates.hearing_id
    where dates.hearing_id is not null and dates.hearing_date is not null
),
govinfo_rows as (
    select
        identity_map.hearing_id,
        (held_date.value #>> '{}')::date as hearing_date
    from {{ ref('int_govinfo_hearing_identity_map') }} as identity_map
    join {{ ref('stg_govinfo__hearings_packages') }} as packages using (package_id)
    join {{ ref('int_raw_responses__current') }} as raw on raw.raw_id = packages.raw_id
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(raw.response -> 'heldDates') = 'array'
                then raw.response -> 'heldDates'
            else '[]'::jsonb
        end
    ) as held_date(value)
    where (held_date.value #>> '{}') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
)

select distinct hearing_id, hearing_date
from (
    select * from congress_rows
    union all
    select * from govinfo_rows
) as all_rows
where hearing_id is not null and hearing_date is not null
