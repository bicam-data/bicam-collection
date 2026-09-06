-- Congress.gov sometimes includes duplicate root URLs in its reported list
-- count. A distinct-identity deficit is source evidence, not proof of an
-- unfetched page, but it is only acceptable when every reported list position
-- and at least one terminal page have actually been stored.
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
  and source_identity_count_gap > 0
  and (
    actionable_missing_item_count > 0
    or terminal_page_count = 0
    or enumerated_row_count + confirmed_source_error_item_count < expected_item_count
  )
