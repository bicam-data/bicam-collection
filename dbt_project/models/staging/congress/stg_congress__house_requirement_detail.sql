{{ config(tags=["house_requirements", "staging"]) }}

with detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        case
            when jsonb_typeof(response -> 'houseRequirement') = 'object' then response -> 'houseRequirement'
            else response
        end as requirement_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'house-requirements'
      and node_type = 'house_requirement_detail'
), matching_responses as (
    {{ resolve_child('house_requirement_detail', 'house_requirement_matching_communications', 'house-requirements') }}
), matching_counts as (
    select
        parent_raw_id,
        max(nullif(response #>> '{pagination,count}', '')::integer) as matching_communications_count
    from matching_responses
    group by 1
)
select
    detail.raw_id,
    detail.endpoint_url,
    detail.raw_response_version_id,
    {{ json_int('requirement_json', 'number') }} as requirement_number,
    {{ json_boolean('requirement_json', 'activeRecord') }} as active_record,
    {{ json_text('requirement_json', 'frequency') }} as frequency,
    {{ json_text('requirement_json', 'legalAuthority') }} as legal_authority,
    {{ json_text('requirement_json', 'nature') }} as nature,
    {{ json_text('requirement_json', 'parentAgency') }} as parent_agency,
    {{ json_text('requirement_json', 'submittingAgency') }} as submitting_agency,
    {{ json_text('requirement_json', 'submittingOfficial') }} as submitting_official,
    matching_counts.matching_communications_count,
    regexp_replace(detail.endpoint_url, '[?].*$', '')
        || '/matching-communications?format=json' as matching_communications_url,
    {{ json_date('requirement_json', 'updateDate') }} as updated_at
from detail
left join matching_counts on matching_counts.parent_raw_id = detail.raw_id
