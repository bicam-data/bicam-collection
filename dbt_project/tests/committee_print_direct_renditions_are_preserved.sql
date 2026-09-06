/* Live Congress.gov committee-print text payloads may place a rendition
   directly in `text` instead of nesting it under `formats`.  The intermediate
   relation must preserve that source shape rather than silently returning an
   empty text table. */
select 1 as missing_direct_rendition
where not exists (
    select 1
    from {{ ref('int_congress_committee_print_text_formats') }}
    where format_json ? 'url'
      and format_json ? 'type'
)
