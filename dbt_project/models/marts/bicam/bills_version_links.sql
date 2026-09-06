{{ config(
    schema='bicam',
    materialized='table',
    tags=['bills', 'versions', 'supplemental'],
    indexes=[
      {'columns': ['source_table', 'source_row_key'], 'unique': true},
      {'columns': ['bill_id']},
      {'columns': ['govinfo_version_code']}
    ]
) }}

/*
   Deterministic links from Congress.gov child rows to the supplemental
   GovInfo bill-version table.  Congress summaries intentionally do not
   appear here: their version_code is a CRS-summary identifier, not a BILLS
   package version code.  Actions are linked only when their displayed text
   exactly names a known Congress bill-text version.  Congress.gov text
   rendition URLs also carry the exact BILLS version suffix; prefer that
   machine-readable code and retain the title-name fallback for historical
   rows whose rendition URL is absent.
*/
with govinfo_versions as (
    select
        bill_id,
        govinfo_package_id,
        version_code,
        row_number() over (
            partition by bill_id, version_code
            order by is_representative_package desc,
                     last_modified desc nulls last,
                     issued_at desc nulls last,
                     govinfo_package_id
        ) as _rn
    from {{ ref('bills_versions') }}
), selected_versions as (
    select bill_id, govinfo_package_id, version_code
    from govinfo_versions
    where _rn = 1
), version_names as (
    select
        bill_id,
        lower(trim(bill_text_version_name)) as version_name,
        upper(trim(bill_text_version_code)) as version_code
    from {{ ref('bills_titles') }}
    where nullif(trim(bill_text_version_name), '') is not null
      and nullif(trim(bill_text_version_code), '') is not null
    group by 1, 2, 3
), unambiguous_version_names as (
    select bill_id, version_name, min(version_code) as version_code
    from version_names
    group by 1, 2
    having count(*) = 1
), title_links as (
    select
        'bills_titles'::text as source_table,
        t.bill_id || '|' || coalesce(t.title_type, '') || '|' || coalesce(t.title, '') || '|' || coalesce(t.bill_text_version_code, '') as source_row_key,
        t.bill_id,
        upper(nullif(trim(t.bill_text_version_code), '')) as source_version_code,
        nullif(trim(t.bill_text_version_name), '') as source_version_name,
        v.govinfo_package_id,
        v.version_code as govinfo_version_code,
        case when v.version_code is null then 'no_matching_govinfo_version' else 'linked' end as link_status,
        'direct_bill_text_version_code'::text as link_rule,
        1.0::numeric as link_confidence
    from {{ ref('bills_titles') }} as t
    left join selected_versions as v
      on v.bill_id = t.bill_id
     and v.version_code = upper(nullif(trim(t.bill_text_version_code), ''))
    where nullif(trim(t.bill_text_version_code), '') is not null
), text_sources as (
    select
        t.*,
        regexp_replace(upper((regexp_match(
            lower(coalesce(t.formatted_text, t.xml, t.pdf)),
            '/bills-[0-9]+[a-z]+[0-9]+([a-z0-9]+)\.(?:html?|xml|pdf)'
        ))[1]), '[0-9]+$', '') as url_version_code
    from {{ ref('bills_texts') }} as t
), text_links as (
    select
        'bills_texts'::text as source_table,
        t.bill_id || '|' || coalesce(t.date::text, '') || '|' || coalesce(t.type, '') as source_row_key,
        t.bill_id,
        coalesce(t.url_version_code, n.version_code) as source_version_code,
        t.type as source_version_name,
        v.govinfo_package_id,
        v.version_code as govinfo_version_code,
        case when v.version_code is null then 'no_matching_govinfo_version' else 'linked' end as link_status,
        case when t.url_version_code is not null
             then 'direct_bill_text_url_version_code'
             else 'exact_bill_text_version_name'
        end::text as link_rule,
        case when t.url_version_code is not null then 1.0 else 0.98 end::numeric as link_confidence
    from text_sources as t
    left join unambiguous_version_names as n
      on n.bill_id = t.bill_id
     and n.version_name = lower(trim(t.type))
    left join selected_versions as v
      on v.bill_id = t.bill_id
     and v.version_code = coalesce(t.url_version_code, n.version_code)
    where t.url_version_code is not null
       or n.version_code is not null
), action_links as (
    select
        'bills_actions'::text as source_table,
        a.action_id::text as source_row_key,
        a.bill_id,
        n.version_code as source_version_code,
        a.text as source_version_name,
        v.govinfo_package_id,
        v.version_code as govinfo_version_code,
        case when v.version_code is null then 'no_matching_govinfo_version' else 'linked' end as link_status,
        'exact_action_text_version_name'::text as link_rule,
        0.90::numeric as link_confidence
    from {{ ref('bills_actions') }} as a
    join unambiguous_version_names as n
      on n.bill_id = a.bill_id
     and n.version_name = lower(trim(a.text))
    left join selected_versions as v
      on v.bill_id = a.bill_id
     and v.version_code = n.version_code
), all_links as (
    select * from title_links
    union all
    select * from text_links
    union all
    select * from action_links
), deduped_links as (
    select
        all_links.*,
        row_number() over (
            partition by source_table, source_row_key
            order by (link_status = 'linked') desc,
                     govinfo_package_id nulls last,
                     govinfo_version_code nulls last
        ) as _rn
    from all_links
)
select
    ({{ hash_to_uuid("source_table || '|' || source_row_key") }})::text as link_id,
    source_table,
    source_row_key,
    bill_id,
    source_version_code,
    source_version_name,
    govinfo_package_id,
    govinfo_version_code,
    link_status,
    link_rule,
    link_confidence
from deduped_links
where _rn = 1
