select
    links.link_id,
    links.source_row_key,
    links.source_version_code,
    links.govinfo_version_code,
    regexp_replace(upper((regexp_match(
        lower(coalesce(texts.formatted_text, texts.xml, texts.pdf)),
        '/bills-[0-9]+[a-z]+[0-9]+([a-z0-9]+)\.(?:html?|xml|pdf)'
    ))[1]), '[0-9]+$', '') as url_version_code
from {{ ref('bills_version_links') }} as links
join {{ ref('bills_texts') }} as texts
  on links.source_table = 'bills_texts'
 and links.source_row_key = texts.bill_id
    || '|' || coalesce(texts.date::text, '')
    || '|' || coalesce(texts.type, '')
where links.source_table = 'bills_texts'
  and links.link_rule = 'direct_bill_text_url_version_code'
  and links.source_version_code is distinct from regexp_replace(upper((regexp_match(
     lower(coalesce(texts.formatted_text, texts.xml, texts.pdf)),
     '/bills-[0-9]+[a-z]+[0-9]+([a-z0-9]+)\.(?:html?|xml|pdf)'
  ))[1]), '[0-9]+$', '')
