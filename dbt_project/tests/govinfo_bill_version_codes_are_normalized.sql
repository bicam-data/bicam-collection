with suffix_cases(suffix, expected_code, expected_variant) as (
    values ('v1rh', 'RH', 1), ('RH2', 'RH', 2), ('ih', 'IH', null::integer),
           ('eas12', 'EAS', 12), ('123', null, null), ('v1rh2', null, null),
           ('eh1s', 'EH', null), ('EH12S', 'EH', null), ('eh0s', null, null),
           ('eh1sx', null, null), ('v1eh1s', null, null),
           ('', null, null), (null, null, null)
), suffix_results as (
    select *, {{ govinfo_bill_version_code('suffix') }} as actual_code,
              {{ govinfo_bill_version_variant('suffix') }} as actual_variant
    from suffix_cases
), url_cases(url, expected_code) as (
    values
      ('https://example.test/BILLS-108hr10v1rh.htm', 'RH'),
      ('https://example.test/BILLS-118hr1rh2.xml?download=1#text', 'RH'),
      ('https://example.test/BILLS-118s1eas12.pdf', 'EAS'),
      ('https://example.test/BILLS-118hr1IH.HTML#section', 'IH'),
      ('https://www.congress.gov/119/bills/hr3426/BILLS-119hr3426eh1s.htm', 'EH'),
      ('https://example.test/BILLS-119hr3426eh1s.xml?download=1#text', 'EH'),
      ('https://example.test/BILLS-119hr3426eh1s.pdf', 'EH'),
      ('https://example.test/BILLS-119hr3426eh1sx.htm', null),
      ('https://example.test/BILLS-118hr1rh.pdf.backup', null),
      ('https://example.test/other.pdf?link=/BILLS-118hr1rh.pdf', null),
      ('https://example.test/BILLS-118hr1v1rh2.htm', null),
      ('https://example.test/BILLS-118hr1rh.txt', null),
      ('https://example.test/other.pdf', null),
      (null, null)
), url_results as (
    select *, {{ govinfo_bill_url_version_code('url') }} as actual_code from url_cases
), package_cases(package_id, expected_code, expected_variant) as (
    values ('BILLS-108hr10v1rh', 'RH', 1), ('BILLS-118hr1rh2', 'RH', 2),
           ('BILLS-119hr3426eh1s', 'EH', null),
           ('BILLS-118hconres12ih', 'IH', null::integer), ('OTHER-118hr1rh', null, null)
), package_results as (
    select *, {{ govinfo_bill_version_code(govinfo_bill_package_suffix('package_id')) }} as actual_code,
              {{ govinfo_bill_version_variant(govinfo_bill_package_suffix('package_id')) }} as actual_variant
    from package_cases
)
select 'suffix' as kind, suffix as input from suffix_results
where actual_code is distinct from expected_code or actual_variant is distinct from expected_variant
union all
select 'starprint', suffix from (values ('eh1s', 1), ('EH12S', 12), ('rh2', null),
    ('v1rh', null), ('eh0s', null), ('ih', null)) as cases(suffix, expected)
where {{ govinfo_bill_starprint_number('suffix') }} is distinct from expected
union all
select 'url', url from url_results where actual_code is distinct from expected_code
union all
select 'package', package_id from package_results
where actual_code is distinct from expected_code or actual_variant is distinct from expected_variant
