{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_meetings'], indexes=[{'columns': ['meeting_id', 'bill_id'], 'unique': true}, {'columns': ['bill_id']}]) }}
select distinct
    {{ bicam_public_committee_meeting_id('meeting_id') }} as meeting_id,
    bill_id
from {{ ref('stg_congress__committee_meeting_bills') }}
where meeting_id is not null and bill_id is not null
