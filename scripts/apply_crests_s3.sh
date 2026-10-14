#!/usr/bin/env bash
# Fill crests in S3 and expose them on fixtures.
#   1. create the assets bucket (public read on crests/* only)
#   2. fetch crests for every team/league/country without one, from the
#      API-Sports media CDN (no API quota; flags use one /countries call)
#   3. coverage
#   4. dbt: fact_fixtures carries home/away_logo_file_id
#   5. check one public crest URL
# Run from the repo root with the Docker stack up. Output: logs/crests_s3.txt
# Needs AWS keys in .env. Safe to rerun: only rows without a crest are fetched.
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
ASSETS_BUCKET="${ASSETS_BUCKET:-ballistics-assets-438542302919-ca-central-1}"
REGION="${AWS_DEFAULT_REGION:-ca-central-1}"
out=logs/crests_s3.txt
mkdir -p logs
psql_run() { docker exec -i ballistics_postgres psql -U "$DB_USER" -d "$DB_NAME" "$@"; }
etl() { docker exec -e ASSETS_BUCKET="$ASSETS_BUCKET" -w /opt/airflow ballistics_airflow_scheduler python -m "$@"; }
{
  echo "== $(date)  bucket=$ASSETS_BUCKET region=$REGION"
  grep -q '^ASSETS_BUCKET=' .env || { printf '\n# Crest images (public read on crests/*)\nASSETS_BUCKET=%s\n' "$ASSETS_BUCKET" >> .env; echo "added ASSETS_BUCKET to .env"; }

  echo "== 1. bucket"
  etl etl.src.update_logos --setup-bucket || exit 1

  echo "== 2. fetch crests"
  etl etl.src.update_logos || exit 1

  echo "== 3. coverage"
  psql_run <<'SQL'
select 'teams' as kind, count(*) as rows, count(logo_file_id) as with_crest from dim.dim_teams
union all select 'leagues', count(*), count(logo_file_id) from dim.dim_leagues
union all select 'countries', count(*), count(logo_file_id) from dim.dim_countries;
SQL

  echo "== 4. dbt: fact_fixtures with crest ids"
  docker compose run --rm --no-deps dbt dbt run -s fact_fixtures \
    || echo "!! dbt run failed (see above)"
  psql_run -c "select count(*) filter (where kickoff_utc >= now()) as upcoming,
                      count(*) filter (where kickoff_utc >= now() and home_logo_file_id is not null
                                        and away_logo_file_id is not null) as both_crests
               from raw.fact_fixtures"

  echo "== 5. public URL check"
  id=$(psql_run -tA -c "select logo_file_id from dim.dim_teams where logo_file_id is not null limit 1")
  url="https://$ASSETS_BUCKET.s3.$REGION.amazonaws.com/crests/teams/$id"
  echo "$url"
  curl -sI "$url" | grep -iE '^(HTTP|content-type|cache-control)'
  echo "== done"
} 2>&1 | tee "$out"
