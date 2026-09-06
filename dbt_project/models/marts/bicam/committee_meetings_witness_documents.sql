{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_meetings'], indexes=[{'columns': ['meeting_id', 'url'], 'unique': true}, {'columns': ['meeting_id']}]) }}
select distinct on (meeting_id, witness_doc_url)
    {{ bicam_public_committee_meeting_id('meeting_id') }} as meeting_id,
    document_type,
    witness_doc_url as url
from {{ ref('stg_congress__committee_meeting_witness_documents') }}
where meeting_id is not null and witness_doc_url is not null
order by meeting_id, witness_doc_url, document_type nulls last
