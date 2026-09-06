{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'nominations'],
    indexes=[
      {'columns': ['nomination_id'], 'unique': true},
      {'columns': ['congress', 'nomination_number']}
    ]
) }}

/*
  Congress.gov exposes a nomination part as the addressable detail record.
  int_nominations__congress deliberately carries that part identity forward;
  keep it as the public key so multiple parts of one nomination cannot
  collapse into one row.
*/
select
    {{ bicam_public_nomination_id('canonical_nomination_id') }} as nomination_id,
    nomination_number::integer as nomination_number,
    nullif(nomination_part, '')::integer as part_number,
    congress::integer as congress,
    description,
    is_privileged,
    is_civilian,
    received_at::date as received_at,
    authority_date::date as authority_date,
    case
        when executive_calendar_number ~ '^[0-9]+$'
            then executive_calendar_number::integer
        else null::integer
    end as executive_calendar_number,
    citation,
    committees_count::integer as committees_count,
    actions_count::integer as actions_count,
    updated_at::timestamptz as updated_at
from {{ ref('int_nominations__congress') }}
where canonical_nomination_id is not null
