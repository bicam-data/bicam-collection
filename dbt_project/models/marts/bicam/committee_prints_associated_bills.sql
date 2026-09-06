{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_prints'], indexes=[{'columns': ['print_id', 'bill_id'], 'unique': true}, {'columns': ['bill_id']}]) }}

select distinct
    coalesce(public_ids.public_print_id, lower(prints.print_id)) as print_id,
    prints.bill_id
from {{ ref('stg_congress__committee_print_bills') }} as prints
left join {{ ref('int_committee_print_public_id_map') }} as public_ids
  on public_ids.source_print_id = lower(prints.print_id)
where prints.print_id is not null and prints.bill_id is not null
