SELECT fixture_id,api_league_id,season,kickoff_utc,home_team_id,away_team_id,
 home_team_fulltime_goal,away_team_fulltime_goal FROM raw.raw_fixtures
 WHERE fixture_status='FT' AND kickoff_utc>='2020-01-01' AND kickoff_utc<'2026-10-05'
 AND home_team_fulltime_goal IS NOT NULL AND away_team_fulltime_goal IS NOT NULL
 ORDER BY kickoff_utc,fixture_id