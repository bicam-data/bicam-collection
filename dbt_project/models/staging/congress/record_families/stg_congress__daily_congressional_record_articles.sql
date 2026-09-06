{{ config(tags=["daily_congressional_record", "staging"]) }}

with source_rows as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'daily-congressional-record'
      and node_type = 'daily_congressional_record_articles'
),
article_sections as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        article.article_json,
        article.article_ordinal,
        section_article.value as section_article_json,
        section_article.ordinal as section_article_ordinal
    from source_rows
    cross join lateral jsonb_array_elements(
        coalesce(response -> 'articles', '[]'::jsonb)
    ) with ordinality as article(article_json, article_ordinal)
    cross join lateral jsonb_array_elements(
        coalesce(article.article_json -> 'sectionArticles', '[]'::jsonb)
    ) with ordinality as section_article(value, ordinal)
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    md5(raw_id::text || ':' || article_ordinal::text || ':' || section_article_ordinal::text) as article_id,
    {{ json_text('article_json', 'name') }} as section_name,
    {{ json_text('section_article_json', 'title') }} as title,
    {{ json_text('section_article_json', 'startPage') }} as start_page,
    {{ json_text('section_article_json', 'endPage') }} as end_page,
    section_article_json -> 'text' as text_formats
from article_sections
