{{ config(severity='warn') }}
-- A group whose ranks repeat can't be shown as a table (the page hides it).
select distinct league_season_id, group_name
from {{ ref('mart_league_standings') }}
where ranks_repeat
