select *
from {{ ref('fct_hearing_matches') }}
where govinfo_source_record_id !~* '^CHRG-[0-9]+[hsj]hrg[0-9]+.*$'
