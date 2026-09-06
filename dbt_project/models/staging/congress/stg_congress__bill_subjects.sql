{{ config(tags=["bills", "staging"]) }}

select
    bill_id,
    subject,
    updated_at
from {{ ref('int_congress_bill_subject_observations') }}
