select *
from {{ ref('fct_bill_matches') }}
where title_agrees is true
  and (
      matched_congress_title is null
      or govinfo_title_field not in ('title', 'short_title')
      or title_agreement_basis not in ('selected_primary', 'congress_title_variant')
  )
