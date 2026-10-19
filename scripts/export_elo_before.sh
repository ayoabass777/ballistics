#!/usr/bin/env bash
# Export pre-match Elo per team per fixture for the pattern-shrink tier test (2026-10-18).
# Usage (repo root, Postgres container running): ./scripts/export_elo_before.sh
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
docker exec -i ballistics_postgres psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "\copy (
  SELECT e.fixture_id, e.team_id, f.is_home, e.kickoff_utc, e.league_name, e.season, e.rating_before
  FROM elo.team_elo_history e
  JOIN raw.engine_head_to_head f ON f.fixture_id = e.fixture_id AND f.team_id = e.team_id
  WHERE e.kickoff_utc >= '2025-01-01'
  ORDER BY e.kickoff_utc
) TO STDOUT WITH CSV HEADER" > data/elo_before.csv
wc -l data/elo_before.csv
