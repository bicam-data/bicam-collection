/*
   Congresses 1–14 contain documented non-authoritative bill numbering and
   can expose both numeric and Roman aliases for the same canonical key. The
   staging relation intentionally preserves each raw detail row; only
   collisions among authoritative (post-standardization) bill numbers fail.
*/
with duplicates as (
    select
        bill_id,
        bool_and(coalesce(congress > 14, false)) as all_rows_authoritative
    from {{ ref('stg_congress__bill_detail') }}
    group by bill_id
    having count(*) > 1
)
select bill_id
from duplicates
where all_rows_authoritative
