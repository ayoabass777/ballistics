-- Minimal pipeline data for the opt-in *_postgres.py tests in CI, run as the
-- database owner after the migrations and the account bootstrap and before
-- deploy/grants.sql (where dbt build sits in the staging deploy).
--
-- raw.fact_fixtures is a dbt model, so the migrations don't create it; this
-- stub carries only the columns the account API reads.
\set ON_ERROR_STOP on
BEGIN;
CREATE TABLE IF NOT EXISTS raw.fact_fixtures (
    fixture_id     bigint PRIMARY KEY,
    fixture_status text,
    kickoff_utc    timestamptz
);
INSERT INTO raw.fact_fixtures (fixture_id, fixture_status, kickoff_utc) VALUES
    (990002001, 'NS', '2099-01-01 12:00+00'),
    (990002002, 'FT', '2020-01-01 12:00+00')
ON CONFLICT DO NOTHING;

INSERT INTO dim.dim_countries (country_id, country_name) VALUES (990001, 'Testland')
ON CONFLICT DO NOTHING;
INSERT INTO dim.dim_leagues (league_id, country_id, league_name, api_league_id) VALUES
    (990001, 990001, 'Test League A', 990001),
    (990002, 990001, 'Test League B', 990002)
ON CONFLICT DO NOTHING;
COMMIT;
