#!/usr/bin/env bash
# Read-only snapshot of what the data detector currently knows.
# Run from the repo root with the Docker stack up. Output: logs/detector_report.txt
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
out=logs/detector_report.txt
mkdir -p logs
q() { docker exec -i ballistics_postgres psql -U "$DB_USER" -d "$DB_NAME" -P pager=off "$@"; }
{
  echo "== $(date)"
  echo "== 1. open alerts (data_quality_events)"
  q -c "select detected_at::timestamp(0) as detected, severity, check_name,
               coalesce(table_schema||'.'||table_name, source_s3_key, '-') as target,
               left(coalesce(error_message, details::text), 140) as message
        from data_detector.data_quality_events
        where status in ('open', 'failed')
        order by detected_at desc limit 40"
  echo "== 2. alerts by check (all time)"
  q -c "select check_name, status, count(*), max(detected_at)::timestamp(0) as latest
        from data_detector.data_quality_events group by 1, 2 order by 1, 2"
  echo "== 3. failed tasks, last 7 days"
  q -c "select updated_at::timestamp(0) as at, dag_id, task_id, run_id,
               left(coalesce(error_message, ''), 140) as error
        from data_detector.pipeline_runs
        where status = 'failed' and updated_at > now() - interval '7 days'
        order by updated_at desc limit 40"
  echo "== 4. latest state per task (last 3 days)"
  q -c "select distinct on (dag_id, task_id) dag_id, task_id, status,
               metadata->>'logical_date' as logical_date, updated_at::timestamp(0) as at
        from data_detector.pipeline_runs
        where updated_at > now() - interval '3 days'
        order by dag_id, task_id, updated_at desc"
  echo "== 5. failed movements, last 7 days"
  q -c "select detected_at::timestamp(0) as at, dag_id, movement_type, target_table,
               failed_count, left(details::text, 120) as details
        from data_detector.data_movements
        where status <> 'success' and detected_at > now() - interval '7 days'
        order by detected_at desc limit 20" 2>&1
  echo "== 6. health summary"
  q -x -c "select * from raw_mart.mart_data_flow_health_summary" 2>&1 | head -40
  echo "== 7. table freshness"
  q -c "select table_schema||'.'||table_name as tbl, row_count, max_updated_at::timestamp(0),
               snapshot_at::timestamp(0)
        from raw_mart.mart_data_flow_table_freshness order by 1" 2>&1
  echo "== 8. daily flow history (if the new check has run)"
  q -c "select check_date, status, count(*) from data_detector.daily_flow_checks
        group by 1, 2 order by 1 desc, 2 limit 20" 2>&1
  echo "== done"
} 2>&1 | tee "$out"
