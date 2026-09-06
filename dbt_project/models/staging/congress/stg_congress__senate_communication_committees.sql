{{ config(tags=["senate_communications", "staging"]) }}

with detail as (
    select
        raw_id,
        case
            when jsonb_typeof(response -> 'senateCommunication') = 'object' then response -> 'senateCommunication'
            else response
        end as communication_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'senate-communications'
      and node_type = 'senate_communication_detail'
), unnested as (
    select
        raw_id,
        communication_json,
        committee.value as committee_json
    from detail
    cross join lateral {{ unnest_array('communication_json', 'committees', 'committee') }}
)
select
    raw_id,
    'senate-' || lower({{ json_text('communication_json', 'communicationType.code') }})
    || '-' || {{ json_text('communication_json', 'number') }}
    || '-' || {{ json_int('communication_json', 'congress') }}::text as communication_id,
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    {{ json_text('committee_json', 'name') }} as committee_name,
    {{ json_date('committee_json', 'referralDate') }} as referral_date,
    {{ json_text('committee_json', 'url') }} as committee_url
from unnested
