/* A leading conjunction is a serialization artifact for these one-person
   GovInfo bill references, not evidence that the source object names several
   legislators.  Require the exact package-level identities to resolve while
   keeping the multi-legislator flag false. */
with expected(package_id, parsed_name, expected_bioguide_id) as (
    values
        ('BILLS-112s3682is', 'and Mr. Durbin', 'D000563'),
        ('BILLS-116hr1112rh', 'and Mr. Khanna', 'K000389'),
        ('BILLS-116hr7791ih', 'and Mr. LaHood)', 'L000585'),
        ('BILLS-116hr2142rh', 'and Mrs. Axne', 'A000378'),
        ('BILLS-112hr3012rh', 'and Mrs. Maloney', 'M000087'),
        ('BILLS-116hr3170rh', 'and Ms. Lofgren', 'L000397')
), observed as (
    select
        package_id,
        parsed_name,
        resolved_bioguide_id,
        multi_legislator_flag
    from {{ ref('bridge_govinfo__member_references') }}
    where package_id in (select package_id from expected)
      and parsed_name in (select parsed_name from expected)
)
select
    expected.package_id,
    expected.parsed_name,
    expected.expected_bioguide_id,
    observed.resolved_bioguide_id,
    observed.multi_legislator_flag
from expected
left join observed
  on observed.package_id = expected.package_id
 and observed.parsed_name = expected.parsed_name
where observed.resolved_bioguide_id is distinct from expected.expected_bioguide_id
   or observed.multi_legislator_flag is distinct from false
