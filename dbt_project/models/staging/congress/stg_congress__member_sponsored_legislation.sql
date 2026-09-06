{{ config(tags=["members", "staging"]) }}

select
    source_raw_id as raw_id,
    member_detail_raw_id as parent_raw_id,
    member_detail_endpoint_url as parent_endpoint_url,
    source_endpoint_url as endpoint_url,
    amendment_number,
    congress,
    introduced_date,
    legislation_type as type,
    latest_action_date,
    latest_action_text,
    legislation_url
from {{ ref('int_congress_member_legislation_observations') }}
where sponsorship_type = 'sponsor'
