#!/bin/bash
# Runs once, when the Postgres data volume is first created (empty).
# Creates Airflow's own database and the two service roles. Table-level
# grants depend on schemas that only exist after the restore or migrations,
# so they live in deploy/grants.sql, which every deploy re-runs.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v airflow_pw="$AIRFLOW_DB_PASSWORD" \
  -v account_pw="$ACCOUNT_DB_PASSWORD" \
  -v research_pw="$RESEARCH_DB_PASSWORD" <<'SQL'
CREATE ROLE airflow LOGIN PASSWORD :'airflow_pw';
CREATE DATABASE airflow OWNER airflow;

-- Account API: reads and writes the app schema, reads the league catalog.
CREATE ROLE account_api LOGIN PASSWORD :'account_pw';

-- Research API: read-only, also enforced per session by its DSN.
CREATE ROLE research_reader LOGIN PASSWORD :'research_pw';
ALTER ROLE research_reader SET default_transaction_read_only = on;
SQL
