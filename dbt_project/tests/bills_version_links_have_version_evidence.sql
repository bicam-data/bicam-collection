select
    link_id,
    source_table,
    source_row_key
from {{ ref('bills_version_links') }}
where link_status = 'linked'
  and (govinfo_package_id is null or govinfo_version_code is null)
