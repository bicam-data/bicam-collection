select link_id, source_table, source_row_key
from {{ ref('bills_version_links') }}
where source_table = 'bills_summaries'
