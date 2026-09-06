select govinfo_source_record_id, chamber, congress_chamber
from {{ ref('fct_hearing_matches') }}
where (
        govinfo_source_record_id ~* '^CHRG-[0-9]+jhrg'
        and chamber <> 'joint'
      )
   or (
        chamber = 'joint'
        and decision = 'accepted'
        and congress_chamber <> 'joint'
      )
