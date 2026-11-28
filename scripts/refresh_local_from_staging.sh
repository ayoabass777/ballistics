#!/usr/bin/env bash
# Refresh the local database's pipeline data from staging (on demand).
#
#   scripts/refresh_local_from_staging.sh          # asks before replacing
#   scripts/refresh_local_from_staging.sh --yes    # no prompt
#
# Staging is the source of truth since 2026-10-27 (local Airflow is stopped).
# This copies the pipeline schemas down so local work (dbt, engine, the local
# account API's Saved view) sees current data:
#
#   1. pg_dump -Fc of the pipeline schemas on staging, streamed over SSH. The
#      app schema (member accounts) and public are never dumped, so no tester
#      data reaches this laptop.
#   2. One local transaction: drop the copied schemas, restore the dump,
#      re-add foreign keys from local schemas into them (app's followed
#      leagues -> dim.dim_leagues). Any error rolls the whole refresh back and
#      the local copy stays as it was.
#   3. Re-create local views outside the copied schemas that the drop removed
#      (legacy public_* dbt views), then ANALYZE.
#
# Local app data (dev accounts, Saved, followed leagues) is left untouched.
# Refuses to run while local Airflow is up: it would write during the copy.
set -Eeuo pipefail

SCHEMAS=(raw raw_stg raw_int raw_mart forecast forecast_archive elo dim
         data_detector raw_config raw_analytics)
HOST="${STAGING_SSH_HOST:-ballistics-v2}"
LOCAL_PG="${LOCAL_PG_CONTAINER:-ballistics_postgres}"
LOCAL_USER="${LOCAL_DB_USER:-ballistics_user}"
LOCAL_DB="${LOCAL_DB_NAME:-ballistics}"
REMOTE_DC='cd /srv/ballistics && docker compose -f deploy/compose.staging.yml --env-file deploy/.env.staging'

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { echo "error: $*" >&2; exit 1; }
lpsql() { docker exec -i "$LOCAL_PG" psql -U "$LOCAL_USER" -d "$LOCAL_DB" -v ON_ERROR_STOP=1 -X -q "$@"; }

YES=0
[[ "${1:-}" == "--yes" ]] && YES=1

# --- preflight -------------------------------------------------------------
docker inspect -f '{{.State.Running}}' "$LOCAL_PG" 2>/dev/null | grep -q true \
  || die "$LOCAL_PG is not running"
if docker ps --format '{{.Names}}' | grep -q '^ballistics_airflow_'; then
  die "local Airflow is running; stop it first (it would write during the refresh)"
fi
ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST" true || die "cannot reach $HOST over SSH"

in_list=$(printf "'%s'," "${SCHEMAS[@]}"); in_list="${in_list%,}"
remote_size=$(ssh -o BatchMode=yes "$HOST" "$REMOTE_DC exec -T postgres psql -U ballistics -d ballistics -Atc \
  \"select pg_size_pretty(sum(pg_total_relation_size(c.oid))) from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname in ($in_list) and c.relkind in ('r','m')\"")
local_fresh=$(lpsql -Atc "select max(updated_at) from raw.raw_fixtures" 2>/dev/null || echo "n/a")
remote_fresh=$(ssh -o BatchMode=yes "$HOST" "$REMOTE_DC exec -T postgres psql -U ballistics -d ballistics -Atc 'select max(updated_at) from raw.raw_fixtures'")

echo "Replace local schemas: ${SCHEMAS[*]}"
echo "  staging data: $remote_size, raw_fixtures updated $remote_fresh"
echo "  local copy:   raw_fixtures updated $local_fresh"
echo "  kept local:   app (accounts, Saved, followed leagues), public, public_*"
if [[ $YES -eq 0 ]]; then
  read -r -p "Proceed? [y/N] " answer
  [[ "$answer" =~ ^[Yy]$ ]] || { echo "aborted"; exit 1; }
fi

WORK="$(mktemp -d "${TMPDIR:-/tmp}/ballistics-refresh.XXXXXX")"
trap 'rm -rf "$WORK"; docker exec "$LOCAL_PG" rm -f /tmp/staging_refresh.dump >/dev/null 2>&1 || true' EXIT

# --- 1. dump on staging ----------------------------------------------------
log "dumping ${#SCHEMAS[@]} schemas on staging"
ns_args=$(printf -- '-n %s ' "${SCHEMAS[@]}")
ssh -o BatchMode=yes "$HOST" "$REMOTE_DC exec -T postgres pg_dump -U ballistics -d ballistics -Fc $ns_args" \
  > "$WORK/staging.dump"
log "dump: $(du -h "$WORK/staging.dump" | cut -f1)"
docker cp -q "$WORK/staging.dump" "$LOCAL_PG:/tmp/staging_refresh.dump"
docker exec "$LOCAL_PG" pg_restore --list /tmp/staging_refresh.dump > "$WORK/toc.txt" \
  || die "dump is not readable"
log "dump verified ($(grep -c 'TABLE DATA' "$WORK/toc.txt") tables)"

# --- 2. what the drop will take with it outside the copied schemas ---------
lpsql -At > "$WORK/fks.sql" <<SQL
select format('ALTER TABLE %s ADD CONSTRAINT %I %s;', c.conrelid::regclass, c.conname,
              pg_get_constraintdef(c.oid))
from pg_constraint c
join pg_class t on t.oid = c.conrelid join pg_namespace tn on tn.oid = t.relnamespace
join pg_class f on f.oid = c.confrelid join pg_namespace fn on fn.oid = f.relnamespace
where c.contype = 'f' and fn.nspname in ($in_list) and tn.nspname not in ($in_list);
SQL
lpsql -At > "$WORK/views.tsv" <<SQL
select c.oid::regclass, replace(pg_get_viewdef(c.oid), E'\n', ' ')
from pg_class c join pg_namespace n on n.oid = c.relnamespace
where c.relkind = 'v' and n.nspname not in ($in_list)
  and n.nspname not in ('pg_catalog', 'information_schema')
order by c.oid;
SQL
log "foreign keys to re-add: $(grep -c . "$WORK/fks.sql" || true); outside views saved: $(grep -c . "$WORK/views.tsv" || true)"

# --- 3. one transaction: drop, restore, re-add keys ------------------------
log "restoring (one transaction; an error leaves the local copy unchanged)"
drop_list=$(printf '%s, ' "${SCHEMAS[@]}"); drop_list="${drop_list%, }"
{
  echo "BEGIN;"
  echo "SET client_min_messages = warning;"
  echo "DROP SCHEMA IF EXISTS $drop_list CASCADE;"
  if ! docker exec "$LOCAL_PG" pg_restore --no-owner --no-acl -f - /tmp/staging_refresh.dump; then
    echo "ROLLBACK;"; exit 1
  fi
  cat "$WORK/fks.sql"
  echo "COMMIT;"
} | lpsql
log "restore committed"

# --- 4. re-create outside views the drop removed, then ANALYZE --------------
restored=0; failed=()
while IFS=$'\t' read -r name def; do
  [[ -z "$name" ]] && continue
  exists=$(lpsql -Atc "select to_regclass('$name') is not null")
  [[ "$exists" == "t" ]] && continue
  if printf 'CREATE VIEW %s AS %s\n' "$name" "$def" | lpsql >/dev/null 2>&1; then
    restored=$((restored + 1))
  else
    failed+=("$name")
  fi
done < "$WORK/views.tsv"
log "outside views re-created: $restored${failed:+; could not re-create: ${failed[*]}}"

lpsql -c "ANALYZE" >/dev/null
log "analyzed"

log "local raw_fixtures now updated $(lpsql -Atc 'select max(updated_at) from raw.raw_fixtures')"
log "done"
