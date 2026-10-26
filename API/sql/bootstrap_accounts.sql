-- Run with psql from any working directory. Includes resolve relative to this file.
\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA IF NOT EXISTS app;
CREATE TABLE IF NOT EXISTS app.app_users (
    user_id TEXT PRIMARY KEY,
    email TEXT,
    first_name TEXT,
    last_name TEXT,
    image_url TEXT,
    auth_provider TEXT NOT NULL DEFAULT 'clerk',
    plan TEXT NOT NULL DEFAULT 'free',
    onboarding_completed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
\ir user_preferences.sql
\ir user_account_data.sql
\ir shared_saved_snapshots.sql
\ir user_followed_leagues.sql
\ir billing.sql
\ir beta_invite_codes.sql
\ir app_events.sql
COMMIT;
