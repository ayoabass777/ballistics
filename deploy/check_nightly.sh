#!/usr/bin/env bash
# Missed-run check for the staging server. Runs on the server from cron,
# outside Airflow: a stopped scheduler (a failed deploy leaves it stopped) also
# stops any DAG that would report it, so this check can't live in Airflow.
#
#   deploy/check_nightly.sh                 # check; alert and exit 1 on a problem
#   deploy/check_nightly.sh install-cron    # 02:30 UTC daily, idempotent
#
# Problems: the Airflow scheduler is not running, Postgres is unreachable, or
# a DAG in NIGHTLY_DAGS has no successful run that ended in the last
# MAX_AGE_HOURS. Alerts go to Slack (ALERT_SLACK_WEBHOOK_URL) and email
# (ALERT_EMAIL_TO via the pipeline's SMTP settings), whichever are set.
# DRY_RUN=1 prints the alert instead of sending it.
set -Eeuo pipefail

ROOT="${BALLISTICS_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
ENV_FILE="$ROOT/deploy/.env.staging"
NIGHTLY_DAGS=(fixture_update_dag engine_predict)
MAX_AGE_HOURS="${MAX_AGE_HOURS:-26}"

set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

compose() { docker compose -f "$ROOT/deploy/compose.staging.yml" --env-file "$ENV_FILE" "$@"; }
log() { printf '%s %s\n' "$(date -u +%FT%H:%MZ)" "$*"; }

# alert SUBJECT BODY: Slack and/or email, never failing the caller.
alert() {
  if [[ "${DRY_RUN:-0}" == 1 ]]; then
    printf 'DRY_RUN alert: %s\n%s\n' "$1" "$2"
    return
  fi
  if [[ -n "${ALERT_SLACK_WEBHOOK_URL:-}" ]]; then
    SUBJECT="$1" BODY="$2" python3 -c 'import json, os; print(json.dumps({"text": "*" + os.environ["SUBJECT"] + "*\n" + os.environ["BODY"]}))' \
      | curl -sS -m 15 -H 'Content-Type: application/json' --data @- "$ALERT_SLACK_WEBHOOK_URL" >/dev/null \
      || echo "slack alert failed" >&2
  fi
  SUBJECT="$1" BODY="$2" python3 - <<'EOF' || echo "alert email failed" >&2
import os, smtplib
from email.message import EmailMessage
to = os.getenv("ALERT_EMAIL_TO")
if not to or not os.getenv("SMTP_USER"):
    raise SystemExit("ALERT_EMAIL_TO / SMTP_USER not set; email not sent")
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

check() {
  local problems=() pg hours dag
  [[ -n "$(compose ps -q --status running airflow-scheduler)" ]] \
    || problems+=("Airflow scheduler is not running (a failed deploy leaves it stopped).")

  pg="$(compose ps -q --status running postgres)"
  if [[ -z "$pg" ]]; then
    problems+=("Postgres is not running.")
  else
    for dag in "${NIGHTLY_DAGS[@]}"; do
      hours="$(docker exec -i "$pg" psql -X -q -At -U "$DB_USER" -d airflow -c \
        "SELECT coalesce(floor(extract(epoch FROM now() - max(end_date)) / 3600), -1)::int
           FROM dag_run WHERE dag_id = '$dag' AND state = 'success'" 2>/dev/null)" \
        || { problems+=("Could not read Airflow's run history for $dag."); continue; }
      if [[ "$hours" == -1 ]]; then
        problems+=("$dag has never succeeded.")
      elif (( hours >= MAX_AGE_HOURS )); then
        problems+=("$dag last succeeded ${hours}h ago (limit ${MAX_AGE_HOURS}h).")
      else
        log "ok: $dag last succeeded ${hours}h ago"
      fi
    done
  fi

  if (( ${#problems[@]} )); then
    local body
    body="$(printf -- '- %s\n' "${problems[@]}")
Host: $(hostname)
Runbook: docs/staging-runbook.md (deploy, rollback, Airflow)."
    log "PROBLEM"; printf '%s\n' "$body"
    alert "[ballistics staging] nightly run missed or Airflow down" "$body"
    return 1
  fi
  log "nightly check passed"
}

install_cron() {
  local line="30 2 * * * $ROOT/deploy/check_nightly.sh >> $HOME/nightly-check.log 2>&1"
  { echo "CRON_TZ=UTC"
    crontab -l 2>/dev/null | grep -v 'deploy/check_nightly.sh' | grep -v '^CRON_TZ=' || true
    echo "$line"
  } | crontab -
  crontab -l
}

case "${1:-check}" in
  check) check ;;
  install-cron) install_cron ;;
  *) sed -n '2,/^set /p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 1 ;;
esac
