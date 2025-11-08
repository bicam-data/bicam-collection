{{ config(materialized='view', tags=['stg','congressional','committeereports']) }}

with flat as (
  {{ select_flatten_payload('congressional_raw', 'committeereports_raw') }}
),
norm as (
  select
    f.*,
    lower(coalesce(f.type, f.reporttype))                                       as report_type,
    nullif(regexp_replace(coalesce(f.number, f.reportnumber), '[^0-9]', '', 'g'), '') as report_number,
    coalesce(f.part, f.documentpart, f.reportpart, '1')                         as report_part_raw,
    coalesce(f.congress, f.congressnumber)                                      as congress_raw,
    coalesce(f.issuedate, f.issue_date)                                         as issuedate_raw
  from flat f
),
final as (
  select
    -- ID format matches CommitteereportsFetcherLogic.extract_item_id
    case when report_type is not null and report_number is not null and congress_raw is not null
      then (report_type || report_number || '-' || coalesce(report_part_raw,'1') || '-' || congress_raw)
      else null end                                                             as report_id,
    {{ govinfo_set_id("(report_type || report_number || '-' || congress_raw)") }} as report_set_id,
    -- attributes
    report_type,
    report_number,
    {{ safe_int('report_part_raw') }}                                           as report_part,
    {{ safe_int('congress_raw') }}                                              as congress,
    {{ clean_long_text('title') }}                                              as title_clean,
    {{ standardize_chamber('chamber') }}                                        as chamber_std,
    {{ safe_timestamp('issuedate_raw') }}                                       as issued_at,
    -- lineage and full payload access
    raw_id,
    source_doc_id,
    scraped_at as processed_at_ts,
    payload
  from norm
)
select * from final
