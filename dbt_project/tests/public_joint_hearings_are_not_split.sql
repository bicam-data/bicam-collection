/*
   Every exact GovInfo JHRG package is one public joint-hearing identity.
   Jacket numbers alone are not unique: multipart and multivolume packages
   legitimately share a jacket number, so those variants must not be treated
   as chamber-split duplicates.
*/
select
    metadata.govinfo_package_id,
    metadata.hearing_id,
    hearings.chamber
from {{ ref('hearings_metadata') }} as metadata
left join {{ ref('hearings') }} as hearings using (hearing_id)
where metadata.govinfo_package_id ~* '^CHRG-[0-9]+jhrg[0-9]+'
  and (
      hearings.hearing_id is null
      or hearings.chamber <> 'joint'
      or hearings.hearing_id !~ '^jhrg'
  )
