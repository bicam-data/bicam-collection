with source_codes as (
    select bill_id || '|' || coalesce(date::text, '') || '|' || coalesce(type, '') as row_key,
           coalesce({{ govinfo_bill_url_version_code('formatted_text') }},
                    {{ govinfo_bill_url_version_code('xml') }},
                    {{ govinfo_bill_url_version_code('pdf') }}) as version_code
    from {{ ref('bills_texts') }}
)
select links.link_id, links.source_row_key, links.source_version_code
from {{ ref('bills_version_links') }} links
where links.source_table = 'bills_texts'
  and links.link_rule = 'direct_bill_text_url_version_code'
  -- Renditions may share a grouping key; require one supporting occurrence.
  and not exists (
      select 1 from source_codes
      where row_key = links.source_row_key and version_code = links.source_version_code
  )
