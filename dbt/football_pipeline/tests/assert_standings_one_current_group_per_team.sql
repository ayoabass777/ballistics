-- Each team in a league season's standings has exactly one current table:
-- the match page shows that table and the team's position in it.
select league_season_id, team_id, count(*) filter (where is_team_current_group) as n_current
from {{ ref('mart_league_standings') }}
where team_id is not null
group by 1, 2
having count(*) filter (where is_team_current_group) <> 1
