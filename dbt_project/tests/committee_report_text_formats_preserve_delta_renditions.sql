/* A complete committee-report text response plus a later non-empty delta must
   retain all source format occurrences.  The public staging relation may
   choose one current locator, but the intermediate evidence must not silently
   discard the earlier PDF or the corrected HTML rendition. */
with formats as (
    select
        format_key,
        format_json
    from {{ ref('int_congress_committee_report_text_formats') }}
    where report_part_id = 'hrpt1-1-118'
), classified as (
    select
        count(*)::bigint as format_count,
        count(distinct format_key)::bigint as distinct_key_count,
        count(*) filter (where format_json ->> 'type' = 'PDF')::bigint as pdf_count,
        count(*) filter (where format_json ->> 'type' = 'Formatted Text')::bigint as html_count,
        count(*) filter (where format_json ->> 'isErrata' = 'Y')::bigint as errata_count
    from formats
)
select *
from classified
where format_count <> 3
   or distinct_key_count <> format_count
   or pdf_count <> 1
   or html_count <> 2
   or errata_count <> 1
