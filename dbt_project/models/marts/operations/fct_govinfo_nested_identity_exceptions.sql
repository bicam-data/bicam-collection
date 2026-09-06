{{ config(
    materialized="table",
    tags=["operations", "quality", "govinfo", "identity_review"]
) }}

select
    raw_id,
    node_type,
    package_id,
    granule_id,
    json_path,
    json_key,
    identity_field,
    raw_value,
    identity_status,
    context,
    'pending_manual_review' as review_status
from {{ ref('int_govinfo_identity_fields') }}
where identity_status in ('invalid_bioguide', 'non_identifier_text')
