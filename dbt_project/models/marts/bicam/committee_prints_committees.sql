{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_prints'], indexes=[{'columns': ['print_id', 'committee_code'], 'unique': true}, {'columns': ['committee_code']}]) }}

select distinct
    coalesce(public_ids.public_print_id, lower(prints.print_id)) as print_id,
    prints.committee_id as committee_code
from {{ ref('stg_congress__committee_print_committees') }} as prints
left join {{ ref('int_committee_print_public_id_map') }} as public_ids
  on public_ids.source_print_id = lower(prints.print_id)
where prints.print_id is not null and prints.committee_id is not null
