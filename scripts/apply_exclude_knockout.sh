#!/usr/bin/env bash
# Apply feat/engine-exclude-knockout: build the engine_* views (fact tables
# minus knockout games), rebuild Elo from scratch on league play only, then
# refit calibration and rerun today's engine (scripts/refit_and_rerun.sh).
# Run from the repo root with the Docker stack up. Output: logs/exclude_knockout.txt
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
out=logs/exclude_knockout.txt
psql_run() { docker exec -i ballistics_postgres psql -U "$DB_USER" -d "$DB_NAME" "$@"; }
ROOT="$(pwd)"
run() {
  docker run --rm --network ballistics_network \
    -v "$ROOT/engine:/opt/engine" -v "$ROOT/etl:/opt/etl" -v "$ROOT/data:/opt/data" \
    -e DB_NAME -e DB_USER -e DB_PASSWORD -e DB_PORT \
    -e DB_HOST=ballistics_postgres -e ENGINE_DB_HOST=ballistics_postgres -w /opt \
    ballistics-engine:latest "$@"
}
{
  echo "== $(date)"
  git switch feat/engine-exclude-knockout || exit 1
  echo "== 1. dbt: engine views"
  docker compose run --rm dbt dbt build -s engine_fixtures engine_head_to_head engine_head_to_head_events || exit 1
  echo "== 2. what the engine no longer sees"
  psql_run < etl/sql/diagnostics/engine_knockout_exclusion.sql
  echo "== 3. Elo full rebuild on league play"
  run python -m engine.elo.elo_service --reset 2>&1 | tail -5 || exit 1
  echo "== 4. refit calibration + rerun engine"
  ./scripts/refit_and_rerun.sh 2>&1 | grep -v "^\s*$" | tail -40
  echo "== done"
} 2>&1 | tee "$out"
