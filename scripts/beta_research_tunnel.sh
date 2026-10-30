#!/usr/bin/env bash
# Serve the local frontend from beta data: forward 127.0.0.1:8001 (the local
# frontend's BALLISTICS_API_BASE_URL) to the staging research API over SSH.
#
#   scripts/beta_research_tunnel.sh        # run in a terminal; Ctrl-C stops it
#
# The research API on staging publishes no port (internal Docker network only,
# no auth), so the tunnel goes to the container's address, looked up on the
# server each start because it changes when the container is recreated.
# Accounts (Saved, Settings) stay on the local account API: the local frontend
# signs in with the Clerk development instance, staging's account API only
# accepts the production one. Nothing on staging is changed.
set -euo pipefail

LOCAL_PORT="${LOCAL_PORT:-8001}"
HOST="${STAGING_SSH_HOST:-ballistics-v2}"

if lsof -nP -iTCP:"$LOCAL_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port $LOCAL_PORT is in use (the local research API?). Stop it first:" >&2
  lsof -nP -iTCP:"$LOCAL_PORT" -sTCP:LISTEN >&2
  exit 1
fi

IP="$(ssh -o BatchMode=yes "$HOST" \
  'cd /srv/ballistics && docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" \
     "$(docker compose -f deploy/compose.staging.yml --env-file deploy/.env.staging ps -q research-api)"')"
[[ -n "$IP" ]] || { echo "research-api is not running on $HOST" >&2; exit 1; }

echo "127.0.0.1:$LOCAL_PORT -> $HOST research-api ($IP:8001). Ctrl-C to stop."
exec ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -L "127.0.0.1:$LOCAL_PORT:$IP:8001" "$HOST"
