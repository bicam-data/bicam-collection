/* Repeated full/incremental observations must not inflate the public child.
   This checks exact public payload duplicates while allowing distinct people
   to share the same nomination position ordinal. */
select
    nomination_id,
    ordinal,
    first_name,
    middle_name,
    last_name,
    prefix,
    suffix,
    state,
    effective_date,
    predecessor_name,
    corps_code,
    count(*) as duplicate_count
from {{ ref('nominations_nominees') }}
group by
    nomination_id,
    ordinal,
    first_name,
    middle_name,
    last_name,
    prefix,
    suffix,
    state,
    effective_date,
    predecessor_name,
    corps_code
having count(*) > 1
