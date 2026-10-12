select * from {{ ref('bill_text_source_exceptions') }}
where bill_id is null or source_url is null
   or response_hash !~ '^[0-9a-f]{64}$'
   or expected_congress = observed_congress
   or bill_id not like '%-' || expected_congress::text
   or verified_alternative_url not like '%/' || expected_congress::text || '/bills/%'
