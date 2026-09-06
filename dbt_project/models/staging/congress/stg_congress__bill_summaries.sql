{{ config(tags=["bills", "staging"]) }}

select
    bill_id,
    action_date,
    action_desc,
    text,
    version_code,
    updated_at
from {{ ref('int_congress_bill_summary_observations') }}
