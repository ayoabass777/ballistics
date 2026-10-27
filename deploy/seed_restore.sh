#!/usr/bin/env bash
# Restore a seed dump (from deploy/seed_from_local.sh) into the staging
# Postgres. Run on the server, once, after the repos and deploy/.env.staging
# are in place and before the first full deploy:
#
#   deploy/seed_restore.sh /var/lib/ballistics-seed/ballistics-<stamp>.dump
#
# Starts only the postgres service, refuses to run if the database already
# has tables in raw (never restores over data), restores as the owner role,
# then records migration 0001 as applied so the deploy's `migrate.py up`
# only runs later migrations.
set -Eeuo pipefail

dump="${1:?usage: seed_restore.sh <dump file>}"
[[ -r "$dump" ]] || { echo "cannot read $dump" >&2; exit 1; }
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT/deploy/.env.staging"
compose() { docker compose -f "$ROOT/deploy/compose.staging.yml" --env-file "$ENV_FILE" "$@"; }

set -a; . "$ENV_FILE"; set +a

echo "==> starting postgres"
compose up -d --wait postgres
pg="$(compose ps -q postgres)"
psql() { docker exec -i "$pg" psql -X -q -v ON_ERROR_STOP=1 -U "$DB_USER" -d ballistics "$@"; }

existing=$(psql -Atc "select count(*) from pg_tables where schemaname='raw'")
if [[ "$existing" != "0" ]]; then
  echo "raw already has $existing tables; refusing to restore over existing data" >&2
  exit 1
fi

echo "==> restoring $(du -h "$dump" | cut -f1) (this takes a while)"
start=$(date +%s)
docker exec -i "$pg" pg_restore -U "$DB_USER" -d ballistics \
    --no-owner --no-privileges --exit-on-error < "$dump"
echo "    restored in $(( $(date +%s) - start ))s"

echo "==> recording migration 0001 as applied"
# A plain Python image: the seed runs before the first deploy has built any
# of ours, and migrate.py only needs psycopg2.
docker run --rm --network ballistics_network -v "$ROOT:/repo:ro" -w /repo \
    -e DATABASE_URL="postgresql://$DB_USER:$DB_PASSWORD@ballistics_postgres:5432/ballistics" \
    python:3.11-slim sh -c "pip install -q --root-user-action=ignore psycopg2-binary \
      && python scripts/migrate.py baseline 0001"

psql -Atc "select 'raw tables: '||count(*) from pg_tables where schemaname='raw'
           union all select 'latest complete run: '||coalesce(max(run_ds)::text,'none') from raw.engine_runs where status='complete'"
echo "==> seed restored; now run the full deploy"
