select
    prints.print_id,
    prints.session as public_session,
    govinfo.session as source_session,
    prints.pages as public_pages,
    govinfo.pages as source_pages
from {{ ref('committee_prints') }} as prints
join {{ ref('int_committee_prints__govinfo') }} as govinfo
  on govinfo.canonical_print_id = prints.print_id
 and govinfo.is_representative_package
where prints.session is distinct from govinfo.session
   or prints.pages is distinct from govinfo.pages
