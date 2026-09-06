/* Both member-legislation and amendment-side endpoints must contribute edges
   for each member↔amendment relationship family. */
select relationship_type
from {{ ref('int_congress_member_amendment_relationship_edges') }}
group by relationship_type
having not bool_or(edge_side = 'left')
    or not bool_or(edge_side = 'right')
