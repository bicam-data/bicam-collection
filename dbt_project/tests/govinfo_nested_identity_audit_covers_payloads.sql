/* Every current GovInfo package/granule payload containing a known identity
   key/value pair must have a corresponding row in the recursive identity
   audit. This deliberately checks the payload boundary rather than only the
   curated members[]/committees[] staging arrays, so nested fields remain
   covered when GovInfo adds another sub-object. */
with raw_payloads as (
    select raw_id, response
    from {{ ref('int_raw_responses__current') }}
    where source = 'govinfo'
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
    select raw_id, 'bioGuideId' as json_key, nullif(trim(item #>> '{}'), '') as raw_value
    from raw_payloads
    cross join lateral jsonb_path_query(response, '$.**.bioGuideId') as value(item)
    where nullif(trim(item #>> '{}'), '') is not null
    union
    select raw_id, 'bioguideId', nullif(trim(item #>> '{}'), '')
    from raw_payloads
    cross join lateral jsonb_path_query(response, '$.**.bioguideId') as value(item)
    where nullif(trim(item #>> '{}'), '') is not null
    union
    select raw_id, 'bioguideID', nullif(trim(item #>> '{}'), '')
    from raw_payloads
    cross join lateral jsonb_path_query(response, '$.**.bioguideID') as value(item)
    where nullif(trim(item #>> '{}'), '') is not null
    union
    select raw_id, 'bioGuideID', nullif(trim(item #>> '{}'), '')
    from raw_payloads
    cross join lateral jsonb_path_query(response, '$.**.bioGuideID') as value(item)
    where nullif(trim(item #>> '{}'), '') is not null
    union
    select raw_id, 'bioguide_id', nullif(trim(item #>> '{}'), '')
    from raw_payloads
    cross join lateral jsonb_path_query(response, '$.**.bioguide_id') as value(item)
    where nullif(trim(item #>> '{}'), '') is not null
    union
    select raw_id, 'thomasId', nullif(trim(item #>> '{}'), '')
    from raw_payloads
    cross join lateral jsonb_path_query(response, '$.**.thomasId') as value(item)
    where nullif(trim(item #>> '{}'), '') is not null
    union
    select raw_id, 'thomasID', nullif(trim(item #>> '{}'), '')
    from raw_payloads
    cross join lateral jsonb_path_query(response, '$.**.thomasID') as value(item)
    where nullif(trim(item #>> '{}'), '') is not null
    union
    select raw_id, 'thomas_id', nullif(trim(item #>> '{}'), '')
    from raw_payloads
    cross join lateral jsonb_path_query(response, '$.**.thomas_id') as value(item)
    where nullif(trim(item #>> '{}'), '') is not null
    union
    select raw_id, 'authorityId', nullif(trim(item #>> '{}'), '')
    from raw_payloads
    cross join lateral jsonb_path_query(response, '$.**.authorityId') as value(item)
    where nullif(trim(item #>> '{}'), '') is not null
    union
    select raw_id, 'authorityID', nullif(trim(item #>> '{}'), '')
    from raw_payloads
    cross join lateral jsonb_path_query(response, '$.**.authorityID') as value(item)
    where nullif(trim(item #>> '{}'), '') is not null
    union
    select raw_id, 'authority_id', nullif(trim(item #>> '{}'), '')
    from raw_payloads
    cross join lateral jsonb_path_query(response, '$.**.authority_id') as value(item)
    where nullif(trim(item #>> '{}'), '') is not null
), identity_bearing_payloads as (
    select distinct raw_id
    from identity_values
), audited_values as (
    select distinct raw_id, json_key, raw_value
    from {{ ref('int_govinfo_identity_fields') }}
)
select identity_values.raw_id
from identity_values
left join audited_values
  on audited_values.raw_id = identity_values.raw_id
 and audited_values.json_key = identity_values.json_key
 and audited_values.raw_value = identity_values.raw_value
where audited_values.raw_id is null
union
select identity_bearing_payloads.raw_id
from identity_bearing_payloads
left join audited_values using (raw_id)
where audited_values.raw_id is null
