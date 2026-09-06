{{ config(
    schema='bicam', materialized='table', tags=['bicam_public', 'amendments'],
    indexes=[{'columns': ['amendment_id']}, {'columns': ['date']}]
) }}

select distinct
    amendment_id,
    date::date as date,
    type,
    coalesce(nullif(texts.raw_text, ''), html.text_content) as raw_text,
    pdf,
    html
from {{ ref('stg_congress__amendment_texts') }} as texts
left join public.html_text_successful_current as html
  on html.url = texts.html
 and texts.html !~* '/crec/'
 and html.extraction_status = 'success'
