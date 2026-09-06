{{ config(tags=["bills", "staging"]) }}

select
    bill_id,
    title,
    title_type,
    chamber,
    title_type_code,
    bill_text_version_name,
    bill_text_version_code,
    updated_at
from {{ ref('int_congress_bill_title_observations') }}
