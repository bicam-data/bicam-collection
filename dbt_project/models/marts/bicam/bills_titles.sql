{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id', 'title']}, {'columns': ['title_type']}]) }}

/*
   Congress.gov can repeat one semantic title in overlapping snapshots: an
   older row may have no version metadata while a later row carries the
   authoritative bill-text version (for example, IH / Introduced in House).
   Keep distinct title text/type observations, but choose one deterministic
   metadata row for each declared public key rather than relying on SELECT
   DISTINCT across all columns.
*/
with title_observations as (
    select
        t.*,
        row_number() over (
            partition by t.bill_id, t.title_type, t.title
            order by (nullif(trim(t.bill_text_version_code), '') is not null) desc,
                     (nullif(trim(t.bill_text_version_name), '') is not null) desc,
                     t.updated_at desc nulls last,
                     nullif(trim(t.bill_text_version_code), '') desc nulls last,
                     nullif(trim(t.bill_text_version_name), '') desc nulls last,
                     {{ normalize_chamber('t.chamber') }} desc nulls last,
                     t.title_type_code desc nulls last
        ) as _rn
    from {{ ref('stg_congress__bill_titles') }} as t
    join {{ ref('bills') }} as b using (bill_id)
)
select
    bill_id,
    title,
    title_type,
    bill_text_version_code,
    bill_text_version_name,
    {{ normalize_chamber('chamber') }} as chamber,
    title_type_code
from title_observations
where _rn = 1
