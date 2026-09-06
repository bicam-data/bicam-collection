/*
   The committee-report text expansion depends on the report-detail payload as
   well as the text child payload.  Retain that dependency in the incremental
   relation so a detail-only parent/version repair propagates downstream.
   Fresh databases get the column when dbt creates the relation; existing
   deployments add it here before the next incremental build.
*/
alter table if exists public.int_congress_committee_report_text_formats
    add column if not exists report_source_raw_response_version_id uuid;
