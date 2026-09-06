{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id', 'version_code'], 'unique': true}, {'columns': ['bill_id', 'action_date']}, {'columns': ['version_code']}]) }}

/* Version code is the declared public grain. A repeated child window or a
   later correction must replace the representation for that version, not
   create a second row merely because text or metadata changed. */
with observations as (
    select
        s.bill_id,
        s.action_date,
        s.action_desc,
        s.text,
        s.version_code,
        row_number() over (
            partition by s.bill_id, s.version_code
            order by s.updated_at desc nulls last,
                     s.action_date desc nulls last,
                     s.action_desc desc nulls last,
                     s.text desc nulls last
        ) as _rn
    from {{ ref('stg_congress__bill_summaries') }} as s
    join {{ ref('bills') }} as b using (bill_id)
)
select bill_id, action_date, action_desc, text, version_code
from observations
where _rn = 1
