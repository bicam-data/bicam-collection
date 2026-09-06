{{ config(
    schema='bicam',
    materialized='table',
    tags=['bills', 'public'],
    pre_hook=["set local work_mem='1GB'", "set local max_parallel_workers_per_gather=4"],
    indexes=[
      {'columns': ['bill_id', 'related_bill_id'], 'unique': true},
      {'columns': ['related_bill_id']}
    ]
) }}
select
    r.bill_id,
    r.relatedbill_id as related_bill_id,
    min(lower(r.relationship_identified_by)) as identification_entity
from {{ ref('stg_congress__bill_relatedbills') }} r
join {{ ref('bills') }} b using (bill_id)
where r.relatedbill_id is not null
group by r.bill_id, r.relatedbill_id
