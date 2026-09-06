{{ config(tags=["bills", "historical", "quality", "operations"]) }}

select *
from {{ ref('stg_congress__bill_introductions') }}
where (congress between 6 and 14 and bill_number_authoritative)
   or (congress > 14 and not bill_number_authoritative)
