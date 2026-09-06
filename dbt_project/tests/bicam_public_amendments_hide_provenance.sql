select column_name
from information_schema.columns
where table_schema = 'bicam'
  and table_name like 'amendments%'
  and (
      column_name in ('raw_id', 'raw_response_version_id', 'endpoint_url')
      or column_name like '%source_raw_id%'
      or column_name like '%raw_response_version_id%'
      or column_name like '%endpoint_url%'
  )

