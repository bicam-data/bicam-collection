{{ config(tags=['bills', 'public', 'quality']) }}

with child_ids as (
    select bill_id from {{ ref('bills_actions') }}
    union all select bill_id from {{ ref('bills_actions_committees') }}
    union all select bill_id from {{ ref('bills_actions_recorded_votes') }}
    union all select bill_id from {{ ref('bills_cbocostestimates') }}
    union all select bill_id from {{ ref('bills_cosponsors') }}
    union all select bill_id from {{ ref('bills_laws') }}
    union all select bill_id from {{ ref('bills_metadata') }}
    union all select bill_id from {{ ref('bills_notes') }}
    union all select bill_id from {{ ref('bills_notes_links') }}
    union all select bill_id from {{ ref('bills_related_bills') }}
    union all select bill_id from {{ ref('bills_sponsors') }}
    union all select bill_id from {{ ref('bills_subjects') }}
    union all select bill_id from {{ ref('bills_summaries') }}
    union all select bill_id from {{ ref('bills_texts') }}
    union all select bill_id from {{ ref('bills_titles') }}
    union all select bill_id from {{ ref('bills_reference_codes') }}
    union all select bill_id from {{ ref('bills_reference_laws') }}
    union all select bill_id from {{ ref('bills_reference_statutes') }}
)
select child_ids.bill_id
from child_ids
left join {{ ref('bills') }} on bills.bill_id = child_ids.bill_id
where bills.bill_id is null
