/* Every reviewed GovInfo authority exception must still be present in the
   observed recursive committee map. This prevents a stale exception from
   silently becoming an undocumented identifier. */
select exceptions.source_committee_id
from {{ ref('govinfo_committee_id_exceptions') }} as exceptions
left join {{ ref('int_govinfo_committee_id_map') }} as mapped
  on mapped.source_committee_id = lower(exceptions.source_committee_id)
where mapped.source_committee_id is null
