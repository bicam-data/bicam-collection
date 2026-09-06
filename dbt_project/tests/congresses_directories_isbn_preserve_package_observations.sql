/* The published directory-ISBN surface keeps repeated ISBNs when they come
   from different GovInfo packages.  Guard against reintroducing a
   congress/ISBN-only de-duplication that silently loses package observations. */
with source_rows as (
    select
        congress_number::integer as congress_number,
        govinfo_package_id::text as govinfo_package_id,
        isbn::text as isbn
    from {{ ref('bootstrap_congresses_directories_isbn') }}
    where isbn is not null
), public_rows as (
    select congress_number, govinfo_package_id, isbn
    from {{ ref('congresses_directories_isbn') }}
), missing_source as (
    select congress_number, govinfo_package_id, isbn
    from source_rows
    except
    select congress_number, govinfo_package_id, isbn
    from public_rows
), extra_public as (
    select congress_number, govinfo_package_id, isbn
    from public_rows
    except
    select congress_number, govinfo_package_id, isbn
    from source_rows
)
select * from missing_source
union all
select * from extra_public
