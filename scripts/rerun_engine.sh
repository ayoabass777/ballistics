#!/usr/bin/env bash
# Refresh Elo, re-run the three live models + consensus for today, then print
# Big dog league changes. Poisson is retired (not in run_model). Run from repo root.
set -euo pipefail
ROOT="$(pwd)"; set -a; source .env; set +a
DS="${1:-$(date -u +%Y-%m-%d)}"
run() {
  docker run --rm --network ballistics_network \
    -v "$ROOT/engine:/opt/engine" -v "$ROOT/etl:/opt/etl" -v "$ROOT/data:/opt/data" \
    -e DB_NAME -e DB_USER -e DB_PASSWORD -e DB_PORT \
    -e DB_HOST=ballistics_postgres -e ENGINE_DB_HOST=ballistics_postgres -w /opt \
    ballistics-engine:latest "$@"
}
echo "== refresh Elo"; run python -m engine.elo.elo_service
for m in mismatch bayesian dixon_coles; do
  echo "== model $m"; run python -m engine.eval.run_model --model "$m" --run-ds "$DS" --days 10
done
echo "== consensus"; run python -m engine.eval.consensus_slips --from-picks --run-ds "$DS"
echo "== Big dog leagues (changes only)"; run python -m engine.eval.diagnostics.big_dog_league_records --changed
