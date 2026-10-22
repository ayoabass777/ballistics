-- Service-role grants, re-run by every deploy as the database owner (after
-- migrations, the account bootstrap and dbt, so every schema exists).
-- Idempotent. The roles themselves come from deploy/postgres-init/01-roles.sh.
--
-- Default privileges cover tables the owner creates later, including the
-- ones dbt drops and recreates on every run.
\set ON_ERROR_STOP on

DO $$
DECLARE s text;
BEGIN
  -- Research API: read-only on pipeline data.
  FOREACH s IN ARRAY ARRAY['raw','raw_stg','raw_int','raw_mart','dim','elo','forecast'] LOOP
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = s) THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO research_reader', s);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO research_reader', s);
      EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO research_reader', s);
    END IF;
  END LOOP;

  -- Account API: reads the league/team catalog, card versions and crests.
  FOREACH s IN ARRAY ARRAY['raw','raw_mart','dim','elo'] LOOP
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = s) THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO account_api', s);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO account_api', s);
      EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO account_api', s);
    END IF;
  END LOOP;
END $$;

-- Account API: reads and writes its own schema; no DDL.
GRANT USAGE ON SCHEMA app TO account_api;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA app TO account_api;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA app TO account_api;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA app TO account_api;
ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO account_api;
ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT USAGE, SELECT ON SEQUENCES TO account_api;
