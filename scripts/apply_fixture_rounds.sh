#!/usr/bin/env bash
# Apply feat/fixture-rounds: add raw_fixtures.round, backfill past seasons
# from the API, rebuild fixtures in dbt, report round labels and stages.
# Run from the repo root with the Docker stack up. Output: logs/fixture_rounds.txt
# The backfill makes one API call per league season and is resumable.
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
out=logs/fixture_rounds.txt
psql_run() { docker exec -i ballistics_postgres psql -U "$DB_USER" -d "$DB_NAME" "$@"; }
etl() { docker exec -w /opt/airflow ballistics_airflow_scheduler python -m "$@"; }
{
  echo "== $(date)"
  git switch feat/fixture-rounds || exit 1
  echo "== 1. migration"
  psql_run -v ON_ERROR_STOP=1 < etl/sql/migrations_2026-10-12_fixture_rounds.sql || exit 1
  echo "== 2. pending seasons (no API calls)"
  etl etl.src.backfill_fixture_rounds --dry-run 2>&1 | tail -1
  echo "== 3. backfill rounds"
  etl etl.src.backfill_fixture_rounds 2>&1 | grep -v "pending:" | tail -5 || exit 1
  echo "== 4. dbt: staging appends round (backfilled rows carry a new updated_at),"
  echo "   then everything downstream rebuilds"
  docker compose run --rm dbt dbt run -s stg_raw_fixtures+ \
    || echo "!! dbt run had failures (see above); continuing"
  docker compose run --rm dbt dbt test -s fact_fixtures assert_fixture_stage_classification \
    || echo "!! dbt tests had failures (see above); continuing"
  echo "== 5. round labels and stages"
  psql_run < etl/sql/diagnostics/fixture_rounds.sql
  echo "== done"
} 2>&1 | tee "$out"
