/* Every curated GovInfo committee-ID override must resolve to an observed
   Congress.gov committee detail; otherwise it is only a spelling guess. */
select overrides.source_committee_id, overrides.committee_id
from {{ ref('govinfo_committee_id_overrides') }} as overrides
left join {{ ref('stg_congress__committee_detail') }} as committees
  on lower(committees.committee_id) = lower(overrides.committee_id)
where committees.committee_id is null
