/* The version-grain raw model must propagate a repaired parent_id even when
   the payload hash and last_seen_at are unchanged.  Without this predicate
   in the incremental model, lineage-only repairs remain invisible to all
   version-backed staging models. */
{{ config(tags=["operations", "coverage", "quality"]) }}

select source.id
from {{ source('raw', 'raw_response_versions') }} as source
left join {{ ref('int_raw_response_versions') }} as materialized
  on materialized.raw_response_version_id = source.id
where materialized.raw_response_version_id is null
   or materialized.parent_id is distinct from source.parent_id
