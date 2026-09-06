/* Regression: the API route jacket is 10, but its CHRG addendum belongs to
   publication package CHRG-117hhrg44535 and therefore to the base root. */
select source_hearing_id, format_package_id, public_hearing_id
from {{ ref('int_congress_hearing_public_id_map') }}
where source_hearing_id = 'hhrg10-117'
  and public_hearing_id <> 'hhrg44535-117'
