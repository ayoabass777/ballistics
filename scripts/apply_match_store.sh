#!/usr/bin/env bash
# Create raw.engine_runs / raw.match_analysis and backfill the latest export,
# so GET /api/match/{id} serves engine sections from the database.
# Run from the repo root with the Docker stack up. Output: logs/match_store.txt
# Safe to rerun: a run_ds is replaced atomically. New runs write themselves
# (consensus task); this is only for runs made before the change.
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
out=logs/match_store.txt
mkdir -p logs
psql_run() { docker exec -i ballistics_postgres psql -U "$DB_USER" -d "$DB_NAME" "$@"; }
engine() { docker exec -w /opt/airflow ballistics_airflow_scheduler python -m "$@"; }
latest=$(ls engine/eval/consensus_slips_20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].json 2>/dev/null | sort | tail -1)
{
  echo "== $(date)  latest export: ${latest:-none}"
  [ -n "$latest" ] || { echo "!! no export found"; exit 1; }
  echo "== 1. backfill (creates tables if missing)"
  engine engine.eval.match_store backfill "$latest" || exit 1
  echo "== 2. runs"
  psql_run -c "select run_ds, status, n_fixtures, generated_at, finished_at from raw.engine_runs order by run_ds desc limit 5"
  echo "== 3. sample: one fixture's sections"
  psql_run -c "select fixture_id, payload->'strength' as strength,
                      jsonb_array_length(coalesce(payload->'goals'->'rows','[]')) as goals_rows,
                      pg_column_size(payload) as bytes
               from raw.match_analysis
               where run_ds = (select max(run_ds) from raw.engine_runs where status='complete')
               order by fixture_id limit 3"
  echo "== done"
} 2>&1 | tee "$out"
