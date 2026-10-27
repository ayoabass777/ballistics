#!/usr/bin/env bash
# Nightly Postgres backup for the staging server (docs/staging-deployment-plan.md,
# "Deploy, rollback and backups"). Runs on the server from cron, outside Airflow,
# so a broken scheduler doesn't stop its own backups.
#
#   deploy/backup_postgres.sh                    # dump, verify, upload (cron runs this)
#   deploy/backup_postgres.sh restore-test [DAY] # restore a backup (default: newest)
#                                                # into a scratch database and compare
#   deploy/backup_postgres.sh install-cron       # 03:30 UTC nightly, idempotent
#
# Backup: pg_dump -Fc of the ballistics and airflow databases → pg_restore --list
# to prove each file is readable → s3://$BACKUP_BUCKET/postgres/<YYYY-MM-DD>/ →
# sizes compared with the uploaded objects. The bucket expires objects after 14
# days and the IAM user can't delete them. Only the newest dump stays on disk.
# Any failed step emails ALERT_EMAIL_TO through the same SMTP settings as the
# pipeline alerts.
#
# Roles are not in the dumps: on a new server postgres-init/01-roles.sh creates
# them from .env.staging before a restore.
set -Eeuo pipefail
umask 077   # dumps hold member data

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT/deploy/.env.staging"
WORK=/var/lib/ballistics-backup
AWS_CLI_IMAGE=amazon/aws-cli:2.27.50
DATABASES=(ballistics airflow)

set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a
BUCKET="${BACKUP_BUCKET:-ballistics-stg-backups}"

compose() { docker compose -f "$ROOT/deploy/compose.staging.yml" --env-file "$ENV_FILE" "$@"; }
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
pg_container() {
  local id
  id="$(compose ps -q postgres)"
  [[ -n "$id" ]] || { echo "postgres is not running" >&2; return 1; }
  printf '%s' "$id"
}
# aws CLI in a container: credentials and region from .env.staging, $WORK mounted.
aws() {
  docker run --rm -i -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ca-central-1}" \
    -v "$WORK:$WORK" "$AWS_CLI_IMAGE" "$@"
}

# alert SUBJECT BODY: email with the stdlib, never failing the caller.
alert() {
  SUBJECT="$1" BODY="$2" python3 - <<'EOF' || echo "alert email failed" >&2
import os, smtplib
from email.message import EmailMessage
to = os.getenv("ALERT_EMAIL_TO")
if not to or not os.getenv("SMTP_USER"):
    raise SystemExit("ALERT_EMAIL_TO / SMTP_USER not set; alert not sent")
msg = EmailMessage()
msg["Subject"] = os.environ["SUBJECT"]
msg["From"] = os.getenv("ALERT_EMAIL_FROM") or os.environ["SMTP_USER"]
msg["To"] = to
msg.set_content(os.environ["BODY"])
with smtplib.SMTP(os.getenv("SMTP_HOST", "smtp.gmail.com"), int(os.getenv("SMTP_PORT", "587")), timeout=20) as s:
    s.starttls()
    s.login(os.environ["SMTP_USER"], os.getenv("SMTP_PASSWORD", ""))
    s.send_message(msg)
EOF
}

backup() {
  local day stamp dir pg step="start"
  day="$(date -u +%F)"
  stamp="$(date -u +%Y%m%dT%H%MZ)"
  dir="$WORK/$stamp"
  trap 'alert "[ballistics staging] backup FAILED: $step" "The nightly Postgres backup ($stamp) failed during: $step
Host: $(hostname)
Log: $WORK/backup.log on the server. Nothing older was removed."' ERR

  step="find postgres"; pg="$(pg_container)"
  mkdir -p "$dir"
  local db file bytes remote start
  for db in "${DATABASES[@]}"; do
    file="$dir/$db.dump"
    step="pg_dump $db"; log "$step"; start=$(date +%s)
    docker exec "$pg" pg_dump -U "$DB_USER" -d "$db" --format=custom --compress=6 > "$file"
    bytes=$(stat -c %s "$file")
    log "  $(numfmt --to=iec "$bytes") in $(( $(date +%s) - start ))s"

    step="verify $db dump"
    docker exec -i "$pg" pg_restore --list < "$file" > /dev/null

    step="upload $db"; log "$step"
    remote="s3://$BUCKET/postgres/$day/$stamp-$db.dump"
    aws s3 cp "$file" "$remote" --only-show-errors
    step="check uploaded size of $db"
    [[ "$(aws s3api head-object --bucket "$BUCKET" --key "${remote#s3://$BUCKET/}" \
          --query ContentLength --output text)" == "$bytes" ]]
  done

  # Keep only this dump locally (a quick restore without a download).
  step="remove older local dumps"
  find "$WORK" -mindepth 1 -maxdepth 1 -type d -name '20*' ! -name "$stamp" -exec rm -rf {} +
  trap - ERR
  log "backup $stamp done: s3://$BUCKET/postgres/$day/"
}

# restore_test [DAY]: newest backup of DAY (default: newest overall) → database
# restore_test → table and row counts against the live database → dropped.
restore_test() {
  local day="${1:-}" key pg file start
  pg="$(pg_container)"
  if [[ -z "$day" ]]; then
    day="$(aws s3 ls "s3://$BUCKET/postgres/" | awk '{print $2}' | tr -d / | sort | tail -n 1)"
  fi
  [[ -n "$day" ]] || { echo "no backups in s3://$BUCKET/postgres/" >&2; exit 1; }
  key="$(aws s3 ls "s3://$BUCKET/postgres/$day/" | awk '{print $4}' | grep -- '-ballistics\.dump$' | sort | tail -n 1)"
  [[ -n "$key" ]] || { echo "no ballistics dump for $day" >&2; exit 1; }

  mkdir -p "$WORK/restore-test"
  file="$WORK/restore-test/$key"
  log "downloading s3://$BUCKET/postgres/$day/$key"
  aws s3 cp "s3://$BUCKET/postgres/$day/$key" "$file" --only-show-errors

  psql_owner() { docker exec -i "$pg" psql -X -q -v ON_ERROR_STOP=1 -U "$DB_USER" "$@"; }
  psql_owner -d postgres -c "DROP DATABASE IF EXISTS restore_test"
  psql_owner -d postgres -c "CREATE DATABASE restore_test"
  log "restoring into restore_test"; start=$(date +%s)
  docker exec -i "$pg" pg_restore -U "$DB_USER" -d restore_test \
      --no-owner --no-privileges --exit-on-error < "$file"
  log "  restored in $(( $(date +%s) - start ))s"

  # Tables per schema, plus exact row counts for the tables that matter most.
  # Live rows can grow after the dump (Airflow, testers), so the live side may
  # be ahead; it should never be behind.
  local counts="
    select 'tables in '||schemaname, count(*)::text from pg_tables
     where schemaname not in ('pg_catalog','information_schema') group by 1
    union all select 'rows raw.raw_fixtures', count(*)::text from raw.raw_fixtures
    union all select 'rows raw.engine_runs (complete)', count(*)::text from raw.engine_runs where status='complete'
    union all select 'rows app.'||relname, n::text from (
      select c.relname, (xpath('/row/n/text()', query_to_xml('select count(*) as n from app.'||quote_ident(c.relname), false, true, '')))[1]::text::bigint n
        from pg_class c join pg_namespace s on s.oid=c.relnamespace
       where s.nspname='app' and c.relkind='r') t
    order by 1"
  local restored live
  restored="$(psql_owner -d restore_test -At -F ' = ' -c "$counts")"
  live="$(psql_owner -d ballistics -At -F ' = ' -c "$counts")"
  [[ -n "$restored" && -n "$live" ]] || { echo "count query returned nothing" >&2; exit 1; }
  printf '%s\n' "$restored" | sed 's/^/  /'
  diff -y --suppress-common-lines <(printf '%s\n' "$restored") <(printf '%s\n' "$live") \
    && log "restore_test matches live ballistics" \
    || log "differences above: left = restored backup, right = live now"

  psql_owner -d postgres -c "DROP DATABASE restore_test"
  rm -rf "$WORK/restore-test"
  log "restore test of $key done; scratch database dropped"
}

install_cron() {
  local line="30 3 * * * $ROOT/deploy/backup_postgres.sh >> $WORK/backup.log 2>&1"
  { crontab -l 2>/dev/null | grep -v 'deploy/backup_postgres.sh' | grep -v '^CRON_TZ=' || true
    echo "CRON_TZ=UTC"
    echo "$line"
  } | crontab -
  crontab -l
}

[[ -d "$WORK" && -w "$WORK" ]] || sudo install -d -m 750 -o "$(id -un)" -g "$(id -gn)" "$WORK"

case "${1:-backup}" in
  backup) exec 9>"$WORK/.lock"; flock -n 9 || { log "a backup is already running"; exit 0; }; backup ;;
  restore-test) shift; restore_test "${1:-}" ;;
  install-cron) install_cron ;;
  *) sed -n '2,/^set /p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 1 ;;
esac
