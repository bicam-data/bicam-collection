{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_meetings'], indexes=[{'columns': ['meeting_id', 'committee_code'], 'unique': true}, {'columns': ['committee_code']}]) }}
select distinct
    {{ bicam_public_committee_meeting_id('meeting_id') }} as meeting_id,
    committee_id as committee_code
from {{ ref('stg_congress__committee_meeting_committees') }}
where meeting_id is not null and committee_id is not null
