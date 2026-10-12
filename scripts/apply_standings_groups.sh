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
  echo "== 2b. backfill past seasons still holding pre-groups rows"
  ids=$(psql_run -At -c "select string_agg(distinct league_season_id::text, ' ') from raw.raw_league_standings where group_name = '';")
  echo "league seasons: ${ids:-none}"
  if [ -n "$ids" ]; then
    docker exec -w /opt/airflow ballistics_airflow_scheduler \
      python -m etl.src.update_standings --league-season-ids $ids || exit 1
  fi
  echo "pre-groups rows left: $(psql_run -At -c "select count(*) from raw.raw_league_standings where group_name = '';")"
  echo "== 3. dbt build"
  # dim_teams+ rebuilds and tests everything downstream of the grain fix.
  docker compose run --rm dbt dbt build -s stg_raw_league_standings \
    dim_teams+ mart_league_standings \
    || echo "!! dbt build had failures (see above); continuing to diagnostics"
  echo "== 4. leagues with more than one group"
  psql_run < etl/sql/diagnostics/standings_groups.sql
  echo "== 5. repeated-rank groups"
  psql_run -c "select league_season_id, group_name, rank, count(*) from raw_mart.mart_league_standings group by 1,2,3 having count(*) > 1 order by 1,2,3;"
  echo "== 6. dim_teams grain (want 0)"
  psql_run -c "select count(*) - count(distinct team_id) as dup_team_rows from raw_int.dim_teams;"
  echo "== done"
} 2>&1 | tee "$out"
