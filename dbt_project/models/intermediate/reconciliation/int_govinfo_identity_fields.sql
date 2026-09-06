{{ config(
    materialized="table",
    tags=["govinfo_bills", "govinfo_committee_reports", "govinfo_hearings", "govinfo_committee_prints", "reconciliation", "quality", "identity_review"]
) }}

/* JSONPath's recursive descent (``$.**``) checks every nested object and
   array, including package/granule subfields whose shape varies by year. */
with base as (
    select
        raw_id,
        node_type,
        response,
        nullif(response #>> '{packageId}', '') as package_id,
        nullif(response #>> '{granuleId}', '') as granule_id
    from {{ ref('int_raw_responses__current') }}
    where source = 'govinfo'
      and category in (
          'govinfo-bills',
          'govinfo-committee-reports',
          'govinfo-hearings',
          'govinfo-committee-prints'
      )
      and node_type in (
          'govinfo_bill_package_list',
          'govinfo_bill_package_detail',
          'govinfo_committee_report_package_list',
          'govinfo_committee_report_package_detail',
          'govinfo_committee_report_granule_list',
          'govinfo_committee_report_granule_detail',
          'govinfo_hearing_package_list',
          'govinfo_hearing_package_detail',
          'govinfo_hearing_granule_list',
          'govinfo_hearing_granule_detail',
          'govinfo_committee_print_package_list',
          'govinfo_committee_print_package_detail',
          'govinfo_committee_print_granule_list',
          'govinfo_committee_print_granule_detail'
      )
), identity_values as (
    select base.*, 'bioguide_id' as identity_field, 'bioGuideId' as source_json_key, '$.**.bioGuideId' as path_pattern,
        value #>> '{}' as raw_value, 'member_context' as context
    from base cross join lateral jsonb_path_query(base.response, '$.**.bioGuideId') as item(value)
    union all
    select base.*, 'bioguide_id', 'bioguideId', '$.**.bioguideId', value #>> '{}', 'member_context'
    from base cross join lateral jsonb_path_query(base.response, '$.**.bioguideId') as item(value)
    union all
    select base.*, 'bioguide_id', 'bioguideID', '$.**.bioguideID', value #>> '{}', 'member_context'
    from base cross join lateral jsonb_path_query(base.response, '$.**.bioguideID') as item(value)
    union all
    select base.*, 'bioguide_id', 'bioGuideID', '$.**.bioGuideID', value #>> '{}', 'member_context'
    from base cross join lateral jsonb_path_query(base.response, '$.**.bioGuideID') as item(value)
    union all
    select base.*, 'bioguide_id', 'bioguide_id', '$.**.bioguide_id', value #>> '{}', 'member_context'
    from base cross join lateral jsonb_path_query(base.response, '$.**.bioguide_id') as item(value)
    union all
    select base.*, 'thomas_id', 'thomasId', '$.**.thomasId', value #>> '{}', 'other_context'
    from base cross join lateral jsonb_path_query(base.response, '$.**.thomasId') as item(value)
    union all
    select base.*, 'thomas_id', 'thomasID', '$.**.thomasID', value #>> '{}', 'other_context'
    from base cross join lateral jsonb_path_query(base.response, '$.**.thomasID') as item(value)
    union all
    select base.*, 'thomas_id', 'thomas_id', '$.**.thomas_id', value #>> '{}', 'other_context'
    from base cross join lateral jsonb_path_query(base.response, '$.**.thomas_id') as item(value)
    union all
    select base.*, 'authority_id', 'authorityId', '$.**.authorityId', value #>> '{}', 'other_context'
    from base cross join lateral jsonb_path_query(base.response, '$.**.authorityId') as item(value)
    union all
    select base.*, 'authority_id', 'authorityID', '$.**.authorityID', value #>> '{}', 'other_context'
    from base cross join lateral jsonb_path_query(base.response, '$.**.authorityID') as item(value)
    union all
    select base.*, 'authority_id', 'authority_id', '$.**.authority_id', value #>> '{}', 'other_context'
    from base cross join lateral jsonb_path_query(base.response, '$.**.authority_id') as item(value)
), authority_mappings as (
    select source_committee_id, committee_id, normalization_status
    from {{ ref('int_govinfo_committee_id_map') }}
)
select distinct
    raw_id,
    node_type,
    path_pattern as json_path,
    source_json_key as json_key,
    identity_field,
    nullif(trim(raw_value), '') as raw_value,
    case
        when identity_field = 'bioguide_id'
             and upper(trim(raw_value)) ~ '^[A-Z][0-9]{6}$'
            then 'valid_bioguide'
        when identity_field = 'bioguide_id' then 'invalid_bioguide'
        when identity_field = 'authority_id'
             and authority_mappings.normalization_status = 'reviewed_authority_exception'
            then 'reviewed_authority_exception'
        when identity_field = 'authority_id'
             and authority_mappings.committee_id is not null
             and trim(raw_value) !~* '^[a-z0-9]+$'
            then 'resolved_authority_name'
        when identity_field in ('thomas_id', 'authority_id')
             and trim(raw_value) ~* '^[a-z0-9]+$'
            then 'identifier_candidate'
        when identity_field in ('thomas_id', 'authority_id') then 'non_identifier_text'
        else 'unclassified'
    end as identity_status,
    package_id,
    granule_id,
    context
from identity_values
left join authority_mappings
  on identity_values.identity_field = 'authority_id'
 and authority_mappings.source_committee_id = lower(trim(identity_values.raw_value))
where nullif(trim(raw_value), '') is not null
