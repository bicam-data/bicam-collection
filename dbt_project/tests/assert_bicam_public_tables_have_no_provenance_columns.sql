{{ config(tags=['bicam_public', 'quality']) }}

-- Public relations intentionally omit ingestion and reconciliation internals.
-- A row here identifies a leaked implementation column after a build.
select table_name, column_name
from information_schema.columns
where table_schema = 'bicam'
  and (
    column_name in (
        'raw_id', 'raw_response_version_id', 'source_raw_id',
        'source_raw_response_version_id', 'endpoint_url',
        'source_endpoint_url', 'source_record_id', 'match_id',
        'candidate_id', 'decision_rule', 'source_name'
    )
    or column_name like '%source_raw_id%'
    or column_name like '%raw_response_version_id%'
    or column_name like '%endpoint_url%'
  )
