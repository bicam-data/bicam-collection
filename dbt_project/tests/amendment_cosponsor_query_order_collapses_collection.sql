/*
   Query parameters are transport state, not child-collection identity.  The
   fixture deliberately provides the same amendment-cosponsor payload through
   the canonical URL and an offset-qualified URL.  Resolver selection must
   collapse those requests to one source occurrence before staging.
*/
select amendment_id
from {{ ref('int_congress_amendment_cosponsor_observations') }}
where amendment_id = 'samdt1-118'
group by amendment_id
having count(*) <> 1
