#!/usr/bin/env bash
# Build fact_fixtures on feat/fact-fixtures and compare engine inputs (raw vs fact).
# Output also goes to logs/fact_fixtures_check.txt so Claude can read it.
set -uo pipefail
cd "$(dirname "$0")/.."
out=logs/fact_fixtures_check.txt
{
  echo "== $(date)"
  git switch feat/fact-fixtures || exit 1
  docker compose run --rm dbt dbt run --full-refresh -s stg_raw_fixtures &&
  docker compose run --rm dbt dbt build -s stg_raw_fixtures int_fixtures fact_fixtures &&
  python -m engine.eval.diagnostics.fixtures_lineage_check
  echo "== exit $?"
} 2>&1 | tee "$out"
