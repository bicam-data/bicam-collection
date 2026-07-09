{{ config(tags=["members", "staging"]) }}

with sponsored_legislation as (
    select *
    from {{ ref('stg_congress__member_sponsored_legislation') }}
    where amendment_number is not null
)

select
    raw_id,
    parent_raw_id,
    parent_endpoint_url,
    endpoint_url,
    amendment_number,
    congress,
    introduced_date,
    latest_action_date,
    latest_action_text,
    legislation_url as amendment_url,
    {{ amendment_type_from_url('legislation_url', 'congress', 'amendment_number') }} as type
from sponsored_legislation
