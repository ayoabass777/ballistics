#!/usr/bin/env bash
# Apply the standings-groups change (feat/standings-groups) and report.
# Run from the repo root with the Docker stack up. Output also goes to
# logs/standings_groups.txt so Claude can read it.
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
out=logs/standings_groups.txt
psql_run() { docker exec -i ballistics_postgres psql -U "$DB_USER" -d "$DB_NAME" "$@"; }
{
  echo "== $(date)"
  git switch feat/standings-groups || exit 1
  echo "== 1. migration"
  psql_run -v ON_ERROR_STOP=1 < etl/sql/migrations_2026-10-11_standings_groups.sql || exit 1
  echo "== 2. standings refresh (keeps groups now)"
  docker exec -w /opt/airflow ballistics_airflow_scheduler python -m etl.src.update_standings || exit 1
  echo "== 3. dbt build"
  docker compose run --rm dbt dbt build -s stg_raw_league_standings \
    dim_teams mart_league_standings || exit 1
  echo "== 4. leagues with more than one group"
  psql_run < etl/sql/diagnostics/standings_groups.sql
  echo "== done"
} 2>&1 | tee "$out"
