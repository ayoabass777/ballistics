-- mart_league_standings grain: one row per league season, group and team.
select league_season_id, group_name, team_id, count(*) as n
from {{ ref('mart_league_standings') }}
where team_id is not null
group by 1, 2, 3
having count(*) > 1
