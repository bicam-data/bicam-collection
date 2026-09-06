/* A repeated upstream detail failure must not erase the entity identity when
   the same item was present on a successful Congress.gov list page.  Detail
   fields remain absent, but the lower-priority list-backed intermediate row
   must be present and source-linked. */
{{ config(tags=["operations", "coverage", "quality"]) }}

with source_errors as (
    select distinct
        category,
        node_type,
        endpoint_url
    from {{ source('raw', 'failed_requests') }}
    where http_status in (404, 500)
      and error ilike '%source_error=%'
      and (category, node_type) in (
          ('bills', 'bill_detail'),
          ('amendments', 'amendment_detail')
      )
), list_backed_records as (
    select
        'bills' as category,
        'bill_detail' as node_type,
        source_record_id
    from {{ ref('int_bills__congress') }}
    where source_record_id like 'congress:list:%'

    union all

    select
        'amendments' as category,
        'amendment_detail' as node_type,
        source_record_id
    from {{ ref('int_amendments__congress') }}
    where source_record_id like 'congress:list:%'
)

select
    source_errors.category,
    source_errors.node_type,
    source_errors.endpoint_url
from source_errors
left join list_backed_records
  on list_backed_records.category = source_errors.category
 and list_backed_records.node_type = source_errors.node_type
 and list_backed_records.source_record_id = 'congress:list:' || source_errors.endpoint_url
where list_backed_records.source_record_id is null
