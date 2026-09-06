select source_committee_id, committee_id, normalization_status
from {{ ref('int_govinfo_committee_id_map') }} as mapped
where (normalization_status in ('exact_canonical', 'verified_o_to_zero', 'manual_override')
       and committee_id is null)
   or (normalization_status = 'verified_o_to_zero'
       and committee_id <> regexp_replace(source_committee_id, 'o', '0', 'g'))
   or (normalization_status in ('unmapped_identifier', 'non_identifier_text')
       and committee_id is not null)
