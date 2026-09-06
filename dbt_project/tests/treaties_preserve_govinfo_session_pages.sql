select
    treaties.treaty_id,
    treaties.congress_received_session as public_session,
    govinfo.session as source_session,
    treaties.pages as public_pages,
    govinfo.pages as source_pages
from {{ ref('treaties') }} as treaties
join {{ ref('stg_govinfo__treaty_documents_packages') }} as govinfo
  using (treaty_id)
where treaties.congress_received_session is distinct from govinfo.session
   or treaties.pages is distinct from govinfo.pages
