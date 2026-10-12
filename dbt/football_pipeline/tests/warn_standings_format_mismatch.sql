{{ config(severity='warn') }}
-- League definition (standings_format in metadata.yaml) disagrees with what the
-- API returned. Fix the definition, then the page shows the right table.
select distinct league_season_id, league_id, standings_format, n_groups, format_check
from {{ ref('mart_league_standings') }}
where format_check <> 'ok'
