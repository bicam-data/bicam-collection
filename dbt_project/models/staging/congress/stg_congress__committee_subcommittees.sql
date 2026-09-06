{{ config(tags=["committees", "staging"]) }}

with committee_detail as (
    select
        response -> 'committee' as committee_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'committees'
      and node_type = 'committee_detail'
)

select
    {{ json_text('committee_json', 'parent,systemCode') }} as committee_id,
    {{ json_text('committee_json', 'systemCode') }} as subcommittee_id
from committee_detail
where {{ json_text('committee_json', 'parent,systemCode') }} is not null
