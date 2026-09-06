{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committees'],
    indexes=[
      {'columns': ['committee_code', 'started_at'], 'unique': true},
      {'columns': ['started_at']}
    ]
) }}

select distinct on (committee_id, started_at::date)
    committee_id as committee_code,
    name,
    loc_name,
    started_at::date as started_at,
    ended_at::date as ended_at,
    committee_type,
    establishing_authority,
    su_doc_class_number,
    nara_id,
    loc_linked_data_id,
    null::timestamptz as updated_at
from {{ ref('stg_congress__committee_history') }}
where committee_id is not null
  and started_at is not null
order by
    committee_id,
    started_at::date,
    (ended_at is not null) desc,
    ended_at::date desc nulls last,
    name nulls last
