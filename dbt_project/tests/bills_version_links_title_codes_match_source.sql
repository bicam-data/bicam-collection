select
    links.link_id,
    links.source_row_key,
    links.source_version_code,
    links.govinfo_version_code,
    titles.bill_text_version_code
from {{ ref('bills_version_links') }} as links
join {{ ref('bills_titles') }} as titles
  on links.source_table = 'bills_titles'
 and links.source_row_key = titles.bill_id || '|' || coalesce(titles.title_type, '') || '|' || coalesce(titles.title, '') || '|' || coalesce(titles.bill_text_version_code, '')
where links.source_table = 'bills_titles'
  and links.govinfo_version_code is not null
  and links.govinfo_version_code is distinct from upper(nullif(trim(titles.bill_text_version_code), ''))
