{% macro resolve_child(parent_node_type, child_node_type, category, source_name="raw") -%}
with ranked_children as (
  select
    child.*,
    row_number() over (
      partition by regexp_replace(
        regexp_replace(
          child.endpoint_url,
          '([?&])(fromDateTime|toDateTime)=[^&]*&?',
          '\1',
          'g'
        ),
        '[?&]$',
        ''
      )
      order by child.fetched_at desc, child.id desc
    ) as logical_endpoint_rank
  from {{ source(source_name, "raw_responses") }} as child
  where child.source = 'congress'
    and child.category = '{{ category }}'
    and child.node_type = '{{ child_node_type }}'
)
select
  child.id as raw_id,
  child.parent_id,
  parent.id as parent_raw_id,
  parent.endpoint_url as parent_endpoint_url,
  child.endpoint_url,
  child.response
from ranked_children as child
join {{ source(source_name, "raw_responses") }} as parent
  on child.parent_id = parent.id
where child.logical_endpoint_rank = 1
  and parent.source = 'congress'
  and parent.category = '{{ category }}'
  and parent.node_type = '{{ parent_node_type }}'
{%- endmacro %}
