{{ config(tags=["bills", "staging"]) }}

with detail as (
    select
        id as raw_id,
        endpoint_url,
        response -> 'bill' as bill_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'bills'
      and node_type = 'bill_detail'
)

select
    raw_id,
    endpoint_url,
    {{ build_custom_id([
        "lower(nullif(bill_json #>> '{type}', ''))",
        "bill_json #>> '{number}'"
    ]) }} || '-' || coalesce(bill_json #>> '{congress}', '') as bill_id,
    lower({{ json_text('bill_json', 'type') }}) as bill_type,
    {{ json_int('bill_json', 'number') }} as bill_number,
    {{ json_int('bill_json', 'congress') }} as congress,
    {{ json_text('bill_json', 'title') }} as title,
    {{ json_text('bill_json', 'originChamber') }} as origin_chamber,
    {{ json_date('bill_json', 'introducedDate') }} as introduced_at,
    {{ json_text('bill_json', 'policyArea.name') }} as policy_area,
    case
        when jsonb_typeof(coalesce(bill_json #> '{laws}', '[]'::jsonb)) = 'array'
             and jsonb_array_length(coalesce(bill_json #> '{laws}', '[]'::jsonb)) > 0 then true
        else false
    end as is_law,
    {{ json_text('bill_json', 'constitutionalAuthorityStatementText') }} as constitutional_authority_statement,
    {{ json_int('bill_json', 'actions.count', 0) }} as actions_count,
    {{ json_int('bill_json', 'amendments.count', 0) }} as amendments_count,
    {{ json_int('bill_json', 'committees.count', 0) }} as committees_count,
    {{ json_int('bill_json', 'cosponsors.count', 0) }} as cosponsors_count,
    greatest(
        {{ json_int('bill_json', 'cosponsors.countIncludingWithdrawnCosponsors', 0) }}
        - {{ json_int('bill_json', 'cosponsors.count', 0) }},
        0
    ) as cosponsors_withdrawn_count,
    {{ json_int('bill_json', 'relatedBills.count', 0) }} as relatedbills_count,
    {{ json_int('bill_json', 'subjects.count', 0) }} as subjects_count,
    {{ json_int('bill_json', 'summaries.count', 0) }} as summaries_count,
    {{ json_int('bill_json', 'textVersions.count', 0) }} as texts_count,
    {{ json_int('bill_json', 'titles.count', 0) }} as titles_count,
    {{ json_timestamptz('bill_json', 'updateDate') }} as updated_at,
    {{ json_timestamptz('bill_json', 'updateDateIncludingText') }} as updated_text_at
from detail
