#!/usr/bin/env bash
# Deploy the v2 staging stack (docs/staging-deployment-plan.md, "Deploy,
# rollback and backups"). Run from the laptop:
#
#   deploy/deploy_staging.sh                         # origin/main + the account default
#   deploy/deploy_staging.sh --ballistics-ref v0.3 --account-ref origin/main
#   deploy/deploy_staging.sh --rollback              # redeploy the previous release
#   deploy/deploy_staging.sh --status                # show deploy/RELEASES on the server
#   deploy/deploy_staging.sh --checkout-only         # first deploy: check out only, so
#                                                    # .env.staging and seed_restore.sh
#                                                    # can run before the full deploy
#
# Laptop side: resolves both refs to commits that are already on origin, then
# pushes those commits over SSH into the server checkouts (the server holds no
# GitHub credentials) and runs this same script there in "remote" mode.
#
# Server side, in order; a failed step stops the deploy before the services
# restart:
#   stop Airflow → check out both commits → render .dbt/profiles.yml →
#   build images tagged with the release → start Postgres → migrate.py up →
#   account bootstrap → dbt build → grants.sql → tag engine/dbt :latest →
#   docker compose up -d → record deploy/RELEASES → prune old images.
#
# Rollback redeploys the previous line of RELEASES. Migrations are
# forward-only: rolling back code never undoes a migration.
set -Eeuo pipefail

HOST="${DEPLOY_HOST:-ballistics-v2}"
BALLISTICS_REF="origin/main"
# Until checkpoint step 1 picks the branch staging deploys from.
ACCOUNT_REF="origin/codex/frontend-redesign"
SERVER_ROOT="${DEPLOY_SERVER_ROOT:-/srv/ballistics}"
SERVER_ACCOUNT="${DEPLOY_SERVER_ACCOUNT:-/srv/football_stats_project}"
KEEP_RELEASES=2

log() { printf '\n==> %s\n' "$*"; }
die() { printf 'deploy: %s\n' "$*" >&2; exit 1; }

usage() {
  sed -n '2,/^set /p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'
  exit "${1:-0}"
}

# ---------------------------------------------------------------------------
# Laptop side
# ---------------------------------------------------------------------------

# Prints the full SHA of $2 in repo $1, failing unless origin has it.
resolve_pushed() {
  local repo="$1" ref="$2" sha
  git -C "$repo" fetch --quiet origin
  sha="$(git -C "$repo" rev-parse --verify --quiet "${ref}^{commit}")" \
    || die "$ref is not a commit in $repo"
  [[ -n "$(git -C "$repo" branch -r --contains "$sha")" ]] \
    || die "$ref ($sha) is not on any origin branch in $repo; push it first"
  printf '%s' "$sha"
}

# Makes $2 on the server a git checkout (if it isn't) and pushes commit $3
# of local repo $1 to refs/deploy/release there.
push_commit() {
  local repo="$1" dir="$2" sha="$3"
  ssh "$HOST" "test -d '$dir/.git' || git init --quiet '$dir'"
  git -C "$repo" push --quiet --force "$HOST:$dir" "$sha:refs/deploy/release"
}

local_main() {
  local rollback=0 status=0 prune=1 mode=deploy
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --ballistics-ref) BALLISTICS_REF="$2"; shift 2 ;;
      --account-ref) ACCOUNT_REF="$2"; shift 2 ;;
      --host) HOST="$2"; shift 2 ;;
      --rollback) rollback=1; shift ;;
      --status) status=1; shift ;;
      --no-prune) prune=0; shift ;;
      --checkout-only) mode=checkout; shift ;;
      -h|--help) usage 0 ;;
      *) usage 1 ;;
    esac
  done

  local here account b_sha a_sha
  here="$(cd "$(dirname "$0")/.." && pwd)"
  account="$(cd "$here/../football_stats_project" && pwd)"

  if (( status )); then
    ssh "$HOST" "cat '$SERVER_ROOT/deploy/RELEASES' 2>/dev/null || echo 'no releases yet'"
    return
  fi

  if (( rollback )); then
    # The line before the last one. A rollback appends a line too, so a
    # second rollback returns to the release that was rolled back.
    local lines
    lines="$(ssh "$HOST" "tail -n 2 '$SERVER_ROOT/deploy/RELEASES' 2>/dev/null" || true)"
    [[ "$(wc -l <<<"$lines")" -eq 2 ]] || die "no previous release to roll back to"
    # RELEASES line: <utc time> <release> <ballistics sha> <account sha>
    read -r _ _ b_sha a_sha <<<"$(head -n 1 <<<"$lines")"
    log "Rolling back to ballistics $b_sha, account $a_sha"
  else
    b_sha="$(resolve_pushed "$here" "$BALLISTICS_REF")"
    a_sha="$(resolve_pushed "$account" "$ACCOUNT_REF")"
    log "$([[ $mode == checkout ]] && echo "Checking out" || echo "Deploying") ballistics $BALLISTICS_REF ($b_sha), account $ACCOUNT_REF ($a_sha) on $HOST"
  fi

  push_commit "$here" "$SERVER_ROOT" "$b_sha"
  push_commit "$account" "$SERVER_ACCOUNT" "$a_sha"

  # Run the script as committed at $b_sha, not the laptop's working copy. It
  # runs from a file, not stdin, so commands inside can't swallow the script.
  # shellcheck disable=SC2029  # expanded locally on purpose
  ssh "$HOST" "set -e; f=\$(mktemp); trap 'rm -f \$f' EXIT
    git -C '$SERVER_ROOT' show '$b_sha:deploy/deploy_staging.sh' > \$f
    bash \$f remote $mode $b_sha $a_sha '$SERVER_ROOT' '$SERVER_ACCOUNT' $prune"
}

# ---------------------------------------------------------------------------
# Server side
# ---------------------------------------------------------------------------

# checkout_commits ROOT ACCOUNT B_SHA A_SHA
checkout_commits() {
  git -C "$1" checkout --quiet --force --detach "$3"
  git -C "$2" checkout --quiet --force --detach "$4"
  # data/ is gitignored except the league config; the engine image copies
  # the folder and the bootstrap DAG reads the config.
  mkdir -p "$1/data"
  [[ -f "$1/data/metadata.yaml" ]] || die "data/metadata.yaml is not in $3"
}

remote_main() {
  local mode="$1" b_sha="$2" a_sha="$3" root="$4" account="$5" prune="$6"
  if [[ "$mode" == checkout ]]; then
    log "check out commits"
    checkout_commits "$root" "$account" "$b_sha" "$a_sha"
    log "Checked out; nothing built or started. Next: deploy/.env.staging, deploy/certs, seed_restore.sh, then a full deploy"
    return
  fi
  local release="b${b_sha:0:10}-a${a_sha:0:10}"
  local env_file="$root/deploy/.env.staging"
  local compose=(docker compose -f "$root/deploy/compose.staging.yml" --env-file "$env_file")

  [[ -f "$env_file" ]] || die "$env_file is missing; copy deploy/staging.env.example and fill it in"
  [[ "$(stat -c %a "$env_file" 2>/dev/null || stat -f %Lp "$env_file")" == 600 ]] \
    || die "$env_file must be chmod 600"
  set -a
  # shellcheck disable=SC1090
  . "$env_file"
  set +a
  [[ "$PROJECT_ROOT" == "$root" ]] || die "PROJECT_ROOT in $env_file is $PROJECT_ROOT, expected $root"
  [[ "$ACCOUNT_REPO" == "$account" ]] || die "ACCOUNT_REPO in $env_file is $ACCOUNT_REPO, expected $account"
  # Caddy mounts deploy/certs; on the server it can be a symlink to
  # /etc/ballistics/certs, where the origin certificate was issued.
  [[ -r "$root/deploy/certs/origin.pem" && -r "$root/deploy/certs/origin.key" ]] \
    || die "$root/deploy/certs/origin.pem and origin.key must exist and be readable"
  # The shell environment overrides --env-file, so this tags every image.
  export RELEASE="$release"

  local step
  trap 'printf "\ndeploy: FAILED during: %s\nAirflow stays stopped. Fix and redeploy, or run deploy_staging.sh --rollback.\n" "$step" >&2' ERR

  # Airflow bind-mounts DAGs and code from the checkout, so it must not run
  # against new code before the schema matches. The first deploy has no
  # compose file yet.
  step="stop Airflow"; log "$step"
  if [[ -f "$root/deploy/compose.staging.yml" ]]; then
    "${compose[@]}" stop airflow-scheduler airflow-webserver
  fi

  step="check out commits"; log "$step"
  checkout_commits "$root" "$account" "$b_sha" "$a_sha"

  step="render .dbt/profiles.yml"; log "$step"
  mkdir -p "$root/.dbt" "$root/dbt/football_pipeline/logs"
  (umask 077; envsubst '${DB_USER} ${DB_PASSWORD}' \
    < "$root/deploy/dbt-profiles.yml.tmpl" > "$root/.dbt/profiles.yml")

  step="build images ($release)"; log "$step"
  "${compose[@]}" --profile build build

  step="start Postgres"; log "$step"
  "${compose[@]}" up -d --wait postgres

  # Restored dumps already have the baseline schema; migrate.py would fail on
  # it. Refuse with the fix instead.
  step="check migration state"
  if [[ "$(psql_owner "" -tAc "SELECT to_regclass('raw.raw_fixtures') IS NOT NULL AND to_regclass('public.schema_migrations') IS NULL")" == t ]]; then
    die "database has a schema but no schema_migrations (restored dump?); run 'migrate.py baseline 0001' once, then redeploy"
  fi

  step="migrate.py up"; log "$step"
  "${compose[@]}" run --rm --no-deps -T \
    -e CONNECTION_CHECK_MAX_COUNT=0 -e DB_HOST=postgres \
    -v "$root/scripts:/opt/airflow/scripts:ro" -v "$root/db:/opt/airflow/db:ro" \
    airflow-scheduler python /opt/airflow/scripts/migrate.py up

  step="account bootstrap"; log "$step"
  psql_owner "$account/API/sql" -v ON_ERROR_STOP=1 -q -f /sql/bootstrap_accounts.sql

  step="dbt build"; log "$step"
  "${compose[@]}" --profile build run --rm --no-deps -T \
    -v "$root/dbt/football_pipeline:/opt/dbt" \
    -v "$root/.dbt/profiles.yml:/root/.dbt/profiles.yml:ro" \
    dbt dbt build --profiles-dir /root/.dbt

  step="grants.sql"; log "$step"
  psql_owner "$root/deploy" -v ON_ERROR_STOP=1 -q -f /sql/grants.sql

  step="tag engine and dbt images"; log "$step"
  # The DAGs start ballistics-engine:latest and ballistics-dbt:latest.
  docker tag "ballistics-engine:$release" ballistics-engine:latest
  docker tag "ballistics-dbt:$release" ballistics-dbt:latest

  step="start services"; log "$step"
  "${compose[@]}" up -d --wait --remove-orphans

  step="record release"
  printf '%s %s %s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$release" "$b_sha" "$a_sha" \
    >> "$root/deploy/RELEASES"
  trap - ERR

  if [[ "$prune" == 1 ]]; then
    log "prune images outside the last $KEEP_RELEASES releases"
    prune_images "$root/deploy/RELEASES"
  fi

  log "Deployed $release"
  "${compose[@]}" ps --format 'table {{.Service}}\t{{.Status}}'
}

# psql_owner DIR ARGS...: psql as the database owner in a throwaway postgres
# container on the stack's network, with DIR (if not empty) mounted at /sql.
# Uses remote_main's compose array.
psql_owner() {
  local dir="$1" mount=()
  shift
  [[ -n "$dir" ]] && mount=(-v "$dir:/sql:ro")
  "${compose[@]}" run --rm --no-deps -T -e PGPASSWORD="$DB_PASSWORD" ${mount[@]+"${mount[@]}"} \
    postgres psql -h postgres -U "$DB_USER" -d ballistics "$@"
}

# Removes ballistics-* images tagged with a release (b<sha>-a<sha>) that is
# not among the last KEEP_RELEASES, then dangling images and week-old build
# cache. Never touches :latest or other tags.
prune_images() {
  local keep
  keep="$(tail -n "$KEEP_RELEASES" "$1" | awk '{print $2}')"
  docker image ls --format '{{.Repository}}:{{.Tag}}' \
    | grep -E '^ballistics-[a-z-]+:b[0-9a-f]{10}-a[0-9a-f]{10}$' \
    | while read -r image; do
        grep -qxF "${image#*:}" <<<"$keep" || docker image rm "$image" >/dev/null
      done
  docker image prune -f >/dev/null
  docker builder prune -f --filter until=168h >/dev/null
}

if [[ "${1:-}" == remote ]]; then
  shift
  remote_main "$@"
else
  local_main "$@"
fi
