/* Known transport parameters must not split one logical child collection.
   Unknown query parameters are intentionally left alone because they may be
   part of the source's identity. */
select raw_id, endpoint_url, logical_endpoint
from {{ ref('int_congress_child_response_selection') }}
where logical_endpoint ~ '[?&](format|limit|offset|fromDateTime|toDateTime|api_key)='
