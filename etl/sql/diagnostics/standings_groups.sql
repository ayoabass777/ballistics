-- Read-only. Run after a standings refresh + dbt build. League seasons whose
-- latest standings have more than one group, with the derived format.
SELECT lc.country_name, lc.league_name, lc.season,
       s.standings_format, MAX(s.n_groups) AS n_groups,
       string_agg(DISTINCT s.group_name, ' | ') AS groups,
       bool_or(s.ranks_repeat) AS any_repeated_ranks
FROM raw_mart.mart_league_standings s
JOIN raw_int.int_league_context lc ON lc.league_season_id = s.league_season_id
WHERE s.n_groups > 1
GROUP BY 1, 2, 3, 4
ORDER BY lc.country_name, lc.league_name;
