#!/usr/bin/env bash
# First run of the daily flow check: create the tracker tables, sync the
# dataset registry, and check the last few days (no notifications).
# Run from the repo root with the Docker stack up. Output: logs/data_flow_check.txt
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
out=logs/data_flow_check.txt
mkdir -p logs
psql_run() { docker exec -i ballistics_postgres psql -U "$DB_USER" -d "$DB_NAME" "$@"; }
etl() { docker exec -w /opt/airflow ballistics_airflow_scheduler python -m "$@"; }
{
  echo "== $(date)"
  echo "== 1. registry"
  etl etl.src.data_detector.datasets validate || exit 1
  etl etl.src.data_detector.datasets sync || exit 1
  echo "== 2. last 3 days (no alerts)"
  for d in 3 2 1; do
    ds=$(date -u -v-${d}d +%F 2>/dev/null || date -u -d "-${d} day" +%F)
    etl etl.src.data_detector.daily_flow "$ds" --no-notify | tail -20
  done
  echo "== 3. history"
  psql_run -c "select check_date, status, count(*) from data_detector.daily_flow_checks
               group by 1, 2 order by 1 desc, 2"
  psql_run -c "select check_date, dataset_id, status, blocked_by from data_detector.daily_flow_checks
               where status <> 'ok' order by 1 desc, 2 limit 30"
  echo "== 4. alert channels configured"
  docker exec ballistics_airflow_scheduler python -c "from etl.src.data_detector.notify import configured_channels as c; print(c() or 'none (log only)')"
  echo "== done"
} 2>&1 | tee "$out"
