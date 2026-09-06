/* A source-backed report number can occur in several Serial Set packages.
   Exactly one package is eligible for source matching, while all evidence is
   retained in int_committee_reports__govinfo. */
select
    canonical_report_id,
    count(*) as representative_count
from {{ ref('int_committee_reports__govinfo') }}
where collection_code = 'SERIALSET'
group by canonical_report_id
having count(*) filter (where is_representative_package) <> 1
