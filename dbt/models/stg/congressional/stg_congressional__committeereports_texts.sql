{{ config(materialized='view', tags=['stg','congressional','committeereports']) }}

with flat as (
  {{ select_flatten_payload('congressional_raw', 'committeereports_texts_raw') }}
)
select
  -- ids from payload
  coalesce(report_id, reportid)                                      as report_id,
  {{ govinfo_set_id('coalesce(report_id, reportid)') }}              as report_set_id,
  -- text fields
  {{ clean_long_text('formatted_text') }}                            as formatted_text_clean,
  pdf,
  html,
  -- lineage and payload
  raw_id,
  source_doc_id,
  scraped_at as processed_at_ts,
  payload
from flat
