#!/usr/bin/env bash
# Stream a seed dump of the local database to the staging server. Run from
# the laptop, once, before the first deploy (docs/staging-deployment-plan.md,
# "Seeding staging data"). Nothing is written to the laptop's disk.
#
#   deploy/seed_from_local.sh            # → ballistics-v2:/var/lib/ballistics-seed/
#
# Included: every pipeline schema with its data (raw, dim, elo, forecast,
# forecast_archive, data_detector, dbt's raw_* schemas), and the app schema
# without rows (staging users start empty).
# Excluded: public tables (Airflow metadata, legacy v1 dbt output,
# schema_migrations; seed_restore.sh baselines instead) and the legacy
# public_* dbt schemas.
#
# Then on the server: deploy/seed_restore.sh <dump file>.
set -Eeuo pipefail

HOST="${DEPLOY_HOST:-ballistics-v2}"
LOCAL_PG="${LOCAL_PG_CONTAINER:-ballistics_postgres}"
SEED_DIR=/var/lib/ballistics-seed
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

set -a; . "$ROOT/.env"; set +a
name="ballistics-$(date -u +%Y%m%dT%H%MZ).dump"

ssh -o BatchMode=yes "$HOST" "sudo install -d -m 750 -o sage -g sage $SEED_DIR"

echo "==> streaming $LOCAL_PG → $HOST:$SEED_DIR/$name"
start=$(date +%s)
docker exec "$LOCAL_PG" pg_dump -U "$DB_USER" -d "$DB_NAME" \
    --format=custom --compress=6 --no-owner --no-privileges \
    --exclude-table='public.*' \
    --exclude-schema='public_*' \
    --exclude-table-data='app.*' \
  | ssh -o BatchMode=yes "$HOST" "cat > $SEED_DIR/$name.part && mv $SEED_DIR/$name.part $SEED_DIR/$name && chmod 640 $SEED_DIR/$name"

size=$(ssh -o BatchMode=yes "$HOST" "du -h $SEED_DIR/$name | cut -f1")
echo "==> done in $(( $(date +%s) - start ))s, $size: $SEED_DIR/$name"
echo "    next, on the server: deploy/seed_restore.sh $SEED_DIR/$name"
