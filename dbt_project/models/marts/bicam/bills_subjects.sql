{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id', 'subject'], 'unique': true}, {'columns': ['subject']}]) }}

/* A subject is a membership edge, not a source-observation key. Repeated
   complete/delta child windows can carry the same subject with different
   observation timestamps; keep the newest source representation once. */
with observations as (
    select
        s.bill_id,
        s.subject,
        s.updated_at,
        row_number() over (
            partition by s.bill_id, s.subject
            order by s.updated_at desc nulls last
        ) as _rn
    from {{ ref('stg_congress__bill_subjects') }} as s
    join {{ ref('bills') }} as b using (bill_id)
)
select bill_id, subject, updated_at
from observations
where _rn = 1
