-- Read-only. FT fixtures per league-season: raw vs fact_head_to_head.
-- Shows where the incremental dbt facts are missing history.
WITH raw AS (
  SELECT rf.api_league_id, rf.season, COUNT(*) AS raw_ft
  FROM raw.raw_fixtures rf
  WHERE rf.fixture_status = 'FT'
  GROUP BY 1, 2
), fact AS (
  SELECT r.api_league_id, r.season, COUNT(DISTINCT f.fixture_id) AS fact_ft,
         MIN(f.updated_at) AS fact_min_upd, MAX(f.updated_at) AS fact_max_upd
  FROM raw.fact_head_to_head f
  JOIN raw.raw_fixtures r ON r.fixture_id = f.fixture_id
  WHERE f.is_played
  GROUP BY 1, 2
), stg AS (
  SELECT api_league_id, season, COUNT(*) FILTER (WHERE is_played) AS stg_ft
  FROM raw_stg.stg_raw_fixtures
  GROUP BY 1, 2
)
SELECT l.league_name, c.country_name, raw.season, raw.raw_ft,
       COALESCE(stg.stg_ft, 0) AS stg_ft,
       COALESCE(fact.fact_ft, 0) AS fact_ft,
       (SELECT MIN(updated_at) FROM raw.raw_fixtures x
         WHERE x.api_league_id = raw.api_league_id AND x.season = raw.season) AS raw_min_upd
FROM raw
LEFT JOIN fact USING (api_league_id, season)
LEFT JOIN stg  USING (api_league_id, season)
JOIN dim.dim_leagues l ON l.api_league_id = raw.api_league_id
JOIN dim.dim_countries c ON c.country_id = l.country_id
WHERE COALESCE(fact.fact_ft, 0) < raw.raw_ft * 0.9
ORDER BY c.country_name, l.league_name, raw.season;
