#!/usr/bin/env bash
# One-off: rebuild dbt facts with full history, then rebuild Elo from scratch.
# Run from the repo root while no DAG is running.
set -euo pipefail
ROOT="$(pwd)"
set -a; source .env; set +a

echo "== dbt full refresh (stg_raw_fixtures and everything downstream)"
docker run --rm --network ballistics_network \
  -v "$ROOT/dbt/football_pipeline:/opt/dbt" \
  -v "$ROOT/.dbt/profiles.yml:/root/.dbt/profiles.yml:ro" \
  -e DBT_PROFILES_DIR=/root/.dbt -w /opt/dbt \
  ballistics-dbt:latest dbt run --full-refresh --select stg_raw_fixtures+ --profiles-dir /root/.dbt

echo "== Elo full rebuild"
docker run --rm --network ballistics_network \
  -v "$ROOT/engine:/opt/engine" -v "$ROOT/etl:/opt/etl" -v "$ROOT/data:/opt/data" \
  -e DB_NAME -e DB_USER -e DB_PASSWORD -e DB_PORT \
  -e DB_HOST=ballistics_postgres -e ENGINE_DB_HOST=ballistics_postgres -w /opt \
  ballistics-engine:latest python -m engine.elo.elo_service --reset

echo "== coverage check (expect no rows)"
docker exec -i ballistics_postgres psql -U "$DB_USER" -d "$DB_NAME" < etl/sql/diagnostics/fact_coverage_by_league.sql
