-- Read-only. Fixtures whose teams are missing from dim.dim_teams (should be ~0 if fix 1 is enough).
SELECT COUNT(*) AS ft_fixtures,
       COUNT(*) FILTER (WHERE ht.team_id IS NULL OR at.team_id IS NULL) AS missing_a_team
FROM raw.raw_fixtures r
LEFT JOIN dim.dim_teams ht ON ht.api_team_id = r.home_team_id
LEFT JOIN dim.dim_teams at ON at.api_team_id = r.away_team_id
WHERE r.fixture_status = 'FT';
