/* The public print row may expose one URL per rendition type, but the
   intermediate relation must retain every source text/format occurrence. */
select
    print_id,
    format_key,
    count(*) as observed_rows
from {{ ref('int_congress_committee_print_text_formats') }}
group by print_id, format_key
having count(*) <> 1
