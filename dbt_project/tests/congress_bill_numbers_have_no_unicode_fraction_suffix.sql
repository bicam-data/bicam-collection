select raw_id, bill_id, bill_number
from {{ ref('stg_congress__bill_detail') }}
where bill_id like '%½%'
   or bill_number like '%½%'
