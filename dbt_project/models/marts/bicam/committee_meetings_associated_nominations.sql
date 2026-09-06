{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_meetings'], indexes=[{'columns': ['meeting_id', 'nomination_id'], 'unique': true}, {'columns': ['nomination_id']}]) }}
select distinct
    {{ bicam_public_committee_meeting_id('meeting_id') }} as meeting_id,
    {{ bicam_public_nomination_id('nomination_id') }} as nomination_id
from {{ ref('stg_congress__committee_meeting_nominations') }}
where meeting_id is not null and nomination_id is not null
