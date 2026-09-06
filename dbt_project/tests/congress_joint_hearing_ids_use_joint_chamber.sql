select hearing_id, chamber, source_chamber
from {{ ref('stg_congress__hearing_detail') }}
where (chamber = 'joint' and hearing_id !~ '^jhrg')
   or (chamber <> 'joint' and hearing_id ~ '^jhrg')
