/* Recursive extraction must retain the identity domain for nested values.
   This prevents the member identity domain from being silently relabeled as
   something else, while authority/Thomas values remain generic audit values;
   committee normalization is narrowed to explicit committees[] arrays. */
select raw_id, node_type, json_key, identity_field, context
from {{ ref('int_govinfo_identity_fields') }}
where (identity_field = 'bioguide_id' and context <> 'member_context')
   or (identity_field = 'authority_id' and context <> 'other_context')
   or (identity_field = 'thomas_id' and context <> 'other_context')
