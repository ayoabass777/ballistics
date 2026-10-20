#!/usr/bin/env bash
# Export pre-match Elo + result per team per fixture (league play only) for the
# pattern-shrink tier test and the strength-clustering test (2026-10-18).
# Usage (repo root, Postgres container running): ./scripts/export_elo_before.sh
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
docker exec -i ballistics_postgres psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "\copy (
  SELECT e.fixture_id, e.team_id, f.is_home, e.kickoff_utc, f.league_season_id,
         dl.league_name, COALESCE(dc.country_name, '') AS country, dl.tier, e.season,
         e.rating_before, f.goals_for, f.goals_against,
         COALESCE(e.rating_before_flat, e.rating_before) AS rating_before_flat
  FROM elo.team_elo_history e
  JOIN raw.engine_head_to_head f ON f.fixture_id = e.fixture_id AND f.team_id = e.team_id
  JOIN dim.dim_league_seasons dls ON dls.league_season_id = f.league_season_id
  JOIN dim.dim_leagues dl ON dl.league_id = dls.league_id
  LEFT JOIN dim.dim_countries dc ON dc.country_id = dl.country_id
  WHERE e.kickoff_utc >= '2023-07-01'
  ORDER BY e.kickoff_utc
) TO STDOUT WITH CSV HEADER" > data/elo_before.csv
wc -l data/elo_before.csv
