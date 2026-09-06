{{ config(tags=['govinfo_congressional_directories', 'members', 'staging']) }}

/*
   One row per member object in a Congressional Directory granule.  The
   recursive member-reference model supplies the stable array ordinal and
   evidence-backed identity key; this model adds the CDIR granule shape so
   downstream models can distinguish member pages from directory frontmatter,
   committee pages, and state headings.
*/
select
    refs.source_member_reference_key,
    refs.raw_id,
    refs.endpoint_url,
    refs.raw_response_version_id,
    refs.package_id,
    refs.granule_id,
    refs.source_issued_at,
    refs.member_json,
    refs.member_ordinal,
    refs.bioguide_id_raw,
    refs.authority_id,
    refs.gpo_id,
    refs.parsed_name,
    refs.member_name_raw,
    refs.authority_fnf,
    refs.authority_lnf,
    refs.authority_other,
    refs.state,
    refs.chamber,
    refs.role,
    refs.party,
    refs.congress,
    raw.response->>'granuleClass' as granule_class,
    raw.response->>'subGranuleClass' as sub_granule_class,
    nullif(trim(raw.response->>'state'), '') as granule_state,
    nullif(trim(raw.response->>'district'), '') as granule_district,
    nullif(trim(raw.response->>'title'), '') as granule_title,
    nullif(trim(raw.response->>'year'), '')::integer as directory_year,
    raw.response->>'dateIssued' as directory_date_issued,
    raw.response->>'lastModified' as directory_last_modified,
    nullif(trim(raw.response->>'biography'), '') as biography,
    nullif(trim(raw.response->>'population'), '')::integer as population,
    nullif(trim(raw.response->>'online'), '') as online
from {{ ref('int_govinfo_member_references') }} as refs
join {{ ref('int_raw_responses__current') }} as raw
  on raw.raw_id = refs.raw_id
where refs.category = 'govinfo-congressional-directories'
  and refs.node_type = 'govinfo_congressional_directory_granule_detail'
  and upper(coalesce(raw.response->>'granuleClass', '')) = 'CONGRESSMEMBERSTATE'
