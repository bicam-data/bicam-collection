{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_prints'], indexes=[{'columns': ['print_id'], 'unique': true}]) }}

with mapped as (
select
    coalesce(public_ids.public_print_id, lower(texts.print_id)) as print_id,
    coalesce(nullif(texts.raw_text, ''), html.text_content) as raw_text,
    texts.formatted_text,
    texts.pdf,
    texts.html,
    texts.xml
from {{ ref('stg_congress__committee_print_texts') }} as texts
left join {{ ref('int_committee_print_public_id_map') }} as public_ids
  on public_ids.source_print_id = lower(texts.print_id)
left join public.html_text_successful_current as html
  on html.url = texts.html
 and html.extraction_status = 'success'
where texts.print_id is not null
)

select
    print_id,
    max(raw_text) as raw_text,
    max(formatted_text) as formatted_text,
    max(pdf) as pdf,
    max(html) as html,
    max(xml) as xml,
    null::text as png
from mapped
group by print_id
