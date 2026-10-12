-- Read-only. Where does Greek history drop out? Counts per season at each layer.
SELECT r.season,
  COUNT(*) FILTER (WHERE r.fixture_status = 'FT')                                   AS raw_ft,
  (SELECT COUNT(*) FROM raw_stg.stg_raw_fixtures s
     WHERE s.api_league_id = 197 AND s.season = r.season AND s.is_played)           AS stg_ft,
  (SELECT COUNT(*) FROM raw_int.int_fixtures i
     JOIN raw.raw_fixtures x ON x.fixture_id = i.fixture_id
     WHERE x.api_league_id = 197 AND x.season = r.season AND i.is_played)           AS int_ft,
  (SELECT COUNT(DISTINCT t.fixture_id) FROM raw_int.fact_team_fixtures t
     JOIN raw.raw_fixtures x ON x.fixture_id = t.fixture_id
     WHERE x.api_league_id = 197 AND x.season = r.season AND t.is_played)           AS team_fx_ft,
  (SELECT COUNT(DISTINCT f.fixture_id) FROM raw.fact_head_to_head f
     JOIN raw.raw_fixtures x ON x.fixture_id = f.fixture_id
     WHERE x.api_league_id = 197 AND x.season = r.season AND f.is_played)           AS h2h_ft,
  MIN(r.updated_at) AS raw_min_upd,
  MAX(r.updated_at) AS raw_max_upd
FROM raw.raw_fixtures r
WHERE r.api_league_id = 197
GROUP BY r.season
ORDER BY r.season;

-- Watermarks each incremental model compares against.
SELECT 'stg_raw_fixtures' AS model, MAX(updated_at) FROM raw_stg.stg_raw_fixtures
UNION ALL SELECT 'fact_team_fixtures', MAX(updated_at) FROM raw_int.fact_team_fixtures
UNION ALL SELECT 'fact_head_to_head', MAX(updated_at) FROM raw.fact_head_to_head;
