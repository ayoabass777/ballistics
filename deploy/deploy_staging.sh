#!/usr/bin/env bash
# Deploy the v2 staging stack (docs/staging-deployment-plan.md, "Deploy,
# rollback and backups"). Run from the laptop:
#
#   deploy/deploy_staging.sh                         # origin/main
#   deploy/deploy_staging.sh --ballistics-ref v0.3
#   deploy/deploy_staging.sh --rollback              # redeploy the previous release
#   deploy/deploy_staging.sh --status                # show deploy/RELEASES on the server
#   deploy/deploy_staging.sh --force-dbt             # run dbt build even if no dbt input changed
#   deploy/deploy_staging.sh --checkout-only         # first deploy: check out only, so
#                                                    # .env.staging and seed_restore.sh
#                                                    # can run before the full deploy
#
# Laptop side: resolves a commit already on origin, pushes it over SSH into
# the server checkout (the server holds no GitHub credentials), and runs this
# same script there in "remote" mode.
#
# Server side, in order; a failed step stops the deploy before the services
# restart:
#   stop Airflow → check out the commit → render .dbt/profiles.yml →
#   build images tagged with the release → start Postgres → migrate.py up →
#   account bootstrap → dbt build → grants.sql → tag engine/dbt :latest →
#   docker compose up -d → restart Caddy if the Caddyfile changed → record
#   deploy/RELEASES → prune old images → smoke checks against the public URL.
#   dbt build (about 3 minutes) is skipped when nothing under DBT_INPUTS
#   changed since the last recorded release; the nightly DAG runs dbt anyway.
#
# Rollback redeploys the previous line of RELEASES. Migrations are
# forward-only: rolling back code never undoes a migration.
set -Eeuo pipefail

HOST="${DEPLOY_HOST:-ballistics-v2}"
BALLISTICS_REF="origin/main"
SERVER_ROOT="${DEPLOY_SERVER_ROOT:-/srv/ballistics}"
KEEP_RELEASES=2
# What dbt build depends on: models and seeds, the schema it builds on, the
# app tables the analytics models read, and its profile.
DBT_INPUTS=(dbt db/migrations API/sql deploy/dbt-profiles.yml.tmpl)
# Where the post-deploy smoke checks look (through Cloudflare, like a tester).
PUBLIC_URL="${DEPLOY_PUBLIC_URL:-https://beta.ballistics.football}"

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
  local rollback=0 status=0 prune=1 mode=deploy force_dbt=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --ballistics-ref) BALLISTICS_REF="$2"; shift 2 ;;
      --host) HOST="$2"; shift 2 ;;
      --rollback) rollback=1; shift ;;
      --status) status=1; shift ;;
      --no-prune) prune=0; shift ;;
      --force-dbt) force_dbt=1; shift ;;
      --checkout-only) mode=checkout; shift ;;
      -h|--help) usage 0 ;;
      *) usage 1 ;;
    esac
  done

  local here b_sha
  here="$(cd "$(dirname "$0")/.." && pwd)"

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
    # RELEASES line: <utc time> <release> <ballistics sha>
    read -r _ _ b_sha _ <<<"$(head -n 1 <<<"$lines")"
    log "Rolling back to ballistics $b_sha"
  else
    b_sha="$(resolve_pushed "$here" "$BALLISTICS_REF")"
    log "$([[ $mode == checkout ]] && echo "Checking out" || echo "Deploying") ballistics $BALLISTICS_REF ($b_sha) on $HOST"
  fi

  push_commit "$here" "$SERVER_ROOT" "$b_sha"

  # Run the script as committed at $b_sha, not the laptop's working copy. It
  # runs from a file, not stdin, so commands inside can't swallow the script.
  # shellcheck disable=SC2029  # expanded locally on purpose
  ssh "$HOST" "set -e; f=\$(mktemp); trap 'rm -f \$f' EXIT
    git -C '$SERVER_ROOT' show '$b_sha:deploy/deploy_staging.sh' > \$f
    bash \$f remote $mode $b_sha '$SERVER_ROOT' $prune $force_dbt"
}

# ---------------------------------------------------------------------------
# Server side
# ---------------------------------------------------------------------------

# checkout_commit ROOT SHA
checkout_commit() {
  git -C "$1" checkout --quiet --force --detach "$2"
  # data/ is gitignored except the league config; the engine image copies
  # the folder and the bootstrap DAG reads the config.
  mkdir -p "$1/data"
  [[ -f "$1/data/metadata.yaml" ]] || die "data/metadata.yaml is not in $2"
  [[ -f "$1/frontend_dev/Dockerfile" ]] || die "frontend_dev/ is not in $2"
  [[ -f "$1/API/Dockerfile" ]] || die "API/ is not in $2"
}

# dbt_inputs_changed ROOT PREV_SHA NEW_SHA: true unless git can show that
# nothing under DBT_INPUTS differs (no previous release, or a commit missing
# from the server checkout, counts as changed).
dbt_inputs_changed() {
  [[ -n "$2" ]] || return 0
  git -C "$1" cat-file -e "$2^{commit}" 2>/dev/null || return 0
  ! git -C "$1" diff --quiet "$2" "$3" -- "${DBT_INPUTS[@]}"
}

remote_main() {
  local mode="$1" b_sha="$2" root="$3" prune="$4" force_dbt="${5:-0}"
  if [[ "$mode" == checkout ]]; then
    log "check out commits"
    checkout_commit "$root" "$b_sha"
    log "Checked out; nothing built or started. Next: deploy/.env.staging, deploy/certs, seed_restore.sh, then a full deploy"
    return
  fi
  local release="b${b_sha:0:10}"
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

  # The last successful release, to decide whether dbt build is needed.
  local prev_sha=""
  [[ -f "$root/deploy/RELEASES" ]] && prev_sha="$(awk 'END{print $3}' "$root/deploy/RELEASES")"

  step="check out commits"; log "$step"
  checkout_commit "$root" "$b_sha"

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
  psql_owner "$root/API/sql" -v ON_ERROR_STOP=1 -q -f /sql/bootstrap_accounts.sql

  if [[ "$force_dbt" == 1 ]] || dbt_inputs_changed "$root" "$prev_sha" "$b_sha"; then
    step="dbt build"; log "$step"
    "${compose[@]}" --profile build run --rm --no-deps -T \
      -v "$root/dbt/football_pipeline:/opt/dbt" \
      -v "$root/.dbt/profiles.yml:/root/.dbt/profiles.yml:ro" \
      dbt dbt build --profiles-dir /root/.dbt
  else
    log "dbt build skipped: ${DBT_INPUTS[*]} unchanged since ${prev_sha:0:10} (--force-dbt to run it)"
  fi

  step="grants.sql"; log "$step"
  psql_owner "$root/deploy" -v ON_ERROR_STOP=1 -q -f /sql/grants.sql

  step="tag engine and dbt images"; log "$step"
  # The DAGs start ballistics-engine:latest and ballistics-dbt:latest.
  docker tag "ballistics-engine:$release" ballistics-engine:latest
  docker tag "ballistics-dbt:$release" ballistics-dbt:latest

  step="start services"; log "$step"
  "${compose[@]}" up -d --wait --remove-orphans

  # Caddy reads its config only at start, and up -d doesn't recreate it for a
  # changed bind-mounted file.
  if [[ -z "$prev_sha" ]] || ! git -C "$root" diff --quiet "$prev_sha" "$b_sha" -- deploy/Caddyfile 2>/dev/null; then
    step="restart Caddy (Caddyfile changed)"; log "$step"
    "${compose[@]}" exec -T caddy caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
    "${compose[@]}" restart caddy
  fi

  step="record release"
  printf '%s %s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$release" "$b_sha" \
    >> "$root/deploy/RELEASES"
  trap - ERR

  if [[ "$prune" == 1 ]]; then
    log "prune images outside the last $KEEP_RELEASES releases"
    prune_images "$root/deploy/RELEASES"
  fi

  log "Deployed $release"
  "${compose[@]}" ps --format 'table {{.Service}}\t{{.Status}}'

  log "smoke checks against $PUBLIC_URL"
  if ! smoke_checks "$PUBLIC_URL"; then
    die "smoke checks FAILED for $release. It is running and recorded in RELEASES; fix and redeploy, or run deploy_staging.sh --rollback"
  fi
}

# smoke_checks BASE_URL: what a tester's browser hits first, through
# Cloudflare. Each check retries for about 30 s while services warm up.
smoke_checks() {
  local base="$1" host="${1#https://}" failed=0
  check() {  # check NAME EXPECTED_STATUS CURL_ARGS...
    local name="$1" want="$2" got=""
    shift 2
    for _ in 1 2 3 4 5 6; do
      got="$(curl -sS -o /dev/null -m 15 -w '%{http_code}' "$@" 2>/dev/null || true)"
      [[ "$got" == "$want" ]] && break
      sleep 5
    done
    if [[ "$got" == "$want" ]]; then
      printf '  ok    %-44s %s\n' "$name" "$got"
    else
      printf '  FAIL  %-44s got %s, want %s\n' "$name" "${got:-no answer}" "$want"
      failed=1
    fi
  }
  check "welcome page" 200 "$base/welcome"
  check "http:// redirects to https://" 301 "http://$host/welcome"
  # A fake code must reach the account API (400), not be refused by the
  # frontend's same-origin check (403) or fail upstream (503).
  check "invite claim reaches the account API" 400 -X POST "$base/api/beta/invite" \
    -H "Origin: $base" -H 'Content-Type: application/json' \
    --data '{"code":"SMOKETEST","email":"nobody@example.com"}'
  check "account profile needs a session" 401 "$base/api/account/profile"
  return "$failed"
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

# Removes ballistics-* images tagged with a release (b<sha>) that is
# not among the last KEEP_RELEASES, then dangling images and week-old build
# cache. Never touches :latest or other tags.
prune_images() {
  local keep
  keep="$(tail -n "$KEEP_RELEASES" "$1" | awk '{print $2}')"
  docker image ls --format '{{.Repository}}:{{.Tag}}' \
    | grep -E '^ballistics-[a-z-]+:b[0-9a-f]{10}$' \
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
