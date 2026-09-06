select *
from {{ ref('fct_pagination_coverage') }}
where category in (
    'bills',
    'amendments',
    'members',
    'congresses',
    'hearings',
    'committees',
    'committee-meetings',
    'committee-prints',
    'committee-reports',
    'treaties',
    'nominations'
)
  and (
    actionable_missing_item_count > 0
   or estimated_missing_page_count > 0
   or terminal_page_count = 0
  )
