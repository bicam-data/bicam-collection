{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id', 'date']}, {'columns': ['type']}]) }}
select distinct
    t.bill_id,
    t.date::date as date,
    t.type,
    case when not {{ bill_text_has_congress_conflict("coalesce(nullif(t.raw_text, ''), html.text_content)", 't.bill_id') }}
         then coalesce(nullif(t.raw_text, ''), html.text_content) end as raw_text,
    t.formatted_text,
    t.pdf,
    t.xml
from {{ ref('stg_congress__bill_texts') }} t
join {{ ref('bills') }} using (bill_id)
left join public.html_text_successful_current as html
  on html.url = t.formatted_text
 and html.extraction_status = 'success'
