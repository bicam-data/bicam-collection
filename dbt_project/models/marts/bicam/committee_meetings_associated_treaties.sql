{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_meetings'], indexes=[{'columns': ['meeting_id', 'treaty_id'], 'unique': true}, {'columns': ['treaty_id']}]) }}
select distinct
    {{ bicam_public_committee_meeting_id('meeting_id') }} as meeting_id,
    regexp_replace(lower(treaty_id), '[a-z]+$', '') as treaty_id
from {{ ref('stg_congress__committee_meeting_treaties') }}
where meeting_id is not null and treaty_id is not null
