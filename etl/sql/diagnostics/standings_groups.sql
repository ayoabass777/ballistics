-- Read-only. Run after a standings refresh + dbt build. Lists league seasons
-- whose latest standings have more than one group, with a suggested
-- standings_format for data/metadata.yaml:
--   teams appear in several groups -> split  (regular season + rounds)
--   groups are disjoint            -> conferences
WITH s AS (
  SELECT league_season_id, league_id, group_name, team_id, n_groups, standings_format
  FROM raw_mart.mart_league_standings
), per_season AS (
  SELECT league_season_id, league_id, MAX(n_groups) AS n_groups,
         MAX(standings_format) AS declared,
         string_agg(DISTINCT NULLIF(group_name, ''), ' | ') AS groups,
         COUNT(*) > COUNT(DISTINCT team_id) AS teams_in_many_groups
  FROM s GROUP BY 1, 2
)
SELECT lc.country_name, lc.league_name, lc.season, p.n_groups, p.groups,
       p.declared,
       CASE WHEN p.teams_in_many_groups THEN 'split' ELSE 'conferences' END AS suggested
FROM per_season p
JOIN raw_int.int_league_context lc ON lc.league_season_id = p.league_season_id
WHERE p.n_groups > 1
ORDER BY lc.country_name, lc.league_name;
