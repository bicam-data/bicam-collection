select
    govinfo_package_id,
    bill_id,
    version_code,
    version_variant
from {{ ref('bills_versions') }}
where govinfo_package_id ~* 'v[0-9]+[a-z]+$'
  and (
      version_code is distinct from upper((regexp_match(
          lower(govinfo_package_id), 'v[0-9]+([a-z]+)$'
      ))[1])
      or version_variant is distinct from (
          (regexp_match(lower(govinfo_package_id), 'v([0-9]+)[a-z]+$'))[1]
      )::integer
  )
