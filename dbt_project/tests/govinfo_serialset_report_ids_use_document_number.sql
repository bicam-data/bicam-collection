/* SERIALSET serialSetNumber is catalog evidence, not the report number.
   This regression test protects the published identity convention, e.g.
   HRPT 10, Congress 16 => hrpt10-1-16. */
select
    package_id,
    report_id,
    report_type,
    report_number,
    congress,
    report_part
from {{ ref('stg_govinfo__committeereports_packages') }}
where upper(coalesce(collection_code, '')) = 'SERIALSET'
  and lower(report_type) in ('hrpt', 'srpt', 'erpt', 'ccrpt')
  and report_id <> lower(report_type)
      || report_number::text
      || '-'
      || report_part::text
      || case when coalesce(is_errata, false) then 'e' else '' end
      || '-'
      || congress::text
