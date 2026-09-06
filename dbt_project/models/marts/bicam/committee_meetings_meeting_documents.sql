{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_meetings'], indexes=[{'columns': ['meeting_id', 'url'], 'unique': true}, {'columns': ['meeting_id']}]) }}
select distinct on (meeting_id, meeting_doc_url)
    {{ bicam_public_committee_meeting_id('meeting_id') }} as meeting_id,
    document_name as name,
    document_type,
    description,
    meeting_doc_url as url
from {{ ref('stg_congress__committee_meeting_documents') }}
where meeting_id is not null and meeting_doc_url is not null
order by
    meeting_id,
    meeting_doc_url,
    (document_name is not null) desc,
    (description is not null) desc,
    document_type nulls last,
    document_name nulls last
