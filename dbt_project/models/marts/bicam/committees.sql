{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committees'],
    indexes=[
      {'columns': ['committee_code'], 'unique': true},
      {'columns': ['chamber', 'is_current']}
    ]
) }}

select
    committee_id as committee_code,
    name,
    {{ normalize_chamber('chamber') }} as chamber,
    case
        when committee_type is null then null::boolean
        else committee_type ilike '%subcommittee%'
    end as is_subcommittee,
    is_current,
    bills_count,
    reports_count,
    nominations_count,
    updated_at
from {{ ref('stg_congress__committee_detail') }}
where committee_id is not null
