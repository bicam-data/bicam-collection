/* Public BICAM relations use lowercase chamber text everywhere.  Keep this
   as a single product-wide contract so a newly added source cannot reintroduce
   mixed-case values in one child table while the others remain normalized. */
with violations as (
    select 'amendments.chamber' as relation_name, chamber as chamber_value
    from {{ ref('amendments') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'amendments_actions_recorded_votes.chamber', chamber
    from {{ ref('amendments_actions_recorded_votes') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'bills.origin_chamber', origin_chamber
    from {{ ref('bills') }}
    where origin_chamber is not null and origin_chamber <> lower(origin_chamber)
    union all
    select 'bills.current_chamber', current_chamber
    from {{ ref('bills') }}
    where current_chamber is not null and current_chamber <> lower(current_chamber)
    union all
    select 'bills_actions_recorded_votes.chamber', chamber
    from {{ ref('bills_actions_recorded_votes') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'bills_titles.chamber', chamber
    from {{ ref('bills_titles') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'committee_meetings.chamber', chamber
    from {{ ref('committee_meetings') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'committee_prints.chamber', chamber
    from {{ ref('committee_prints') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'committee_reports.chamber', chamber
    from {{ ref('committee_reports') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'committees.chamber', chamber
    from {{ ref('committees') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'committees_members.chamber', chamber
    from {{ ref('committees_members') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'congresses_sessions.chamber', chamber
    from {{ ref('congresses_sessions') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'hearings.chamber', chamber
    from {{ ref('hearings') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'members_leadership_roles.chamber', chamber
    from {{ ref('members_leadership_roles') }}
    where chamber is not null and chamber <> lower(chamber)
    union all
    select 'members_terms.chamber', chamber
    from {{ ref('members_terms') }}
    where chamber is not null and chamber <> lower(chamber)
)
select relation_name, chamber_value
from violations
