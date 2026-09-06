{{ config(tags=["amendments", "staging"]) }}

with keyed as (
    select
        observations.*,
        row_number() over (
            partition by observations.amendment_detail_raw_id,
                         observations.amendment_type,
                         observations.amendment_number,
                         observations.congress
            order by observations.source_fetched_at desc nulls last,
                     observations.source_raw_id desc,
                     observations.source_ordinal desc
        ) as key_rank,
        observations.amendment_detail_raw_id is not null
            and observations.amendment_type is not null
            and observations.amendment_number is not null
            and observations.congress is not null as key_complete
    from {{ ref('int_congress_amendment_amendment_observations') }} as observations
)

select source_raw_id as raw_id,
       amendment_detail_raw_id as parent_raw_id,
       amendment_detail_endpoint_url as parent_endpoint_url,
       source_endpoint_url as endpoint_url,
       congress, amendment_number, amendment_type, update_date, amendment_url
from keyed
where key_rank = 1 or not key_complete
