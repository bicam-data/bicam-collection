/* A title type/text pair is the public semantic grain. Source-version
   metadata is reconciled deterministically in bills_titles. */
select
    bill_id,
    title_type,
    title
from {{ ref('bills_titles') }}
group by bill_id, title_type, title
having count(*) > 1
