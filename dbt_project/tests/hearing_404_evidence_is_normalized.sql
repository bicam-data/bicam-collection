-- A failed request is persisted as an absolute URL, while
-- fct_hearing_matches compares it with the host-neutral expected API path.
-- This test guards the normalization used by the reconciliation model.
with failed as (
    select
        regexp_replace(
            regexp_replace(endpoint_url, '^https?://[^/]+', ''),
            '[?].*$',
            ''
        ) as endpoint_path
    from (values
        ('https://api.congress.gov/v3/hearing/105/house/57910?format=json'),
        ('https://api.congress.gov/v3/hearing/115/nochamber/35389?format=json')
    ) as failures(endpoint_url)
), expected as (
    select endpoint_path
    from (values
        ('/v3/hearing/105/house/57910'),
        ('/v3/hearing/115/nochamber/35389')
    ) as paths(endpoint_path)
)
select *
from failed
full join expected using (endpoint_path)
where failed.endpoint_path is null
   or expected.endpoint_path is null
