{{ config(tags=["nominations", "staging"]) }}

select
    md5(source_raw_id::text || ':' || source_ordinal::text) as raw_id,
    nomination_detail_raw_id as parent_raw_id,
    nomination_detail_endpoint_url as parent_endpoint_url,
    source_endpoint_url as endpoint_url,
    first_name,
    middle_name,
    last_name,
    nominee_ordinal as ordinal,
    state
from {{ ref('int_congress_nominee_observations') }}
