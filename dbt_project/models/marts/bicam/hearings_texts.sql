{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'hearings'], indexes=[{'columns': ['hearing_id'], 'unique': true}]) }}

with congress_texts as (
    select
        public_ids.public_hearing_id as hearing_id,
        texts.raw_text,
        texts.pdf,
        texts.formatted_text
    from {{ ref('stg_congress__hearing_texts') }} as texts
    join {{ ref('int_congress_hearing_public_id_map') }} as public_ids
      on public_ids.source_hearing_id = texts.hearing_id
    where texts.hearing_id is not null
),
congress_texts_with_html as (
    select
        congress_texts.hearing_id,
        coalesce(nullif(congress_texts.raw_text, ''), html.text_content) as raw_text,
        congress_texts.pdf,
        congress_texts.formatted_text
    from congress_texts
    left join public.html_text_successful_current as html
      on html.url = congress_texts.formatted_text
     and html.extraction_status = 'success'
),
govinfo_texts as (
    select
        hearing_id,
        null::text as raw_text,
        nullif(granule_json #>> '{download,pdfLink}', '') as pdf,
        nullif(granule_json #>> '{download,txtLink}', '') as formatted_text
    from {{ ref('int_govinfo_hearing_granules') }}
),
all_texts as (
    select * from congress_texts_with_html
    union all
    select * from govinfo_texts
)

select
    hearing_id,
    max(raw_text) as raw_text,
    max(pdf) as pdf,
    max(formatted_text) as formatted_text
from all_texts
where hearing_id is not null
  and coalesce(raw_text, pdf, formatted_text) is not null
group by hearing_id
