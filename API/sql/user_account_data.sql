-- Account-owned research data. Run after app.app_users exists.
CREATE TABLE IF NOT EXISTS app.saved_fixtures (
    user_id TEXT NOT NULL REFERENCES app.app_users(user_id) ON DELETE CASCADE,
    fixture_id BIGINT NOT NULL CHECK (fixture_id > 0),
    card JSONB NOT NULL,
    run_ds TEXT,
    saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, fixture_id)
);

CREATE INDEX IF NOT EXISTS saved_fixtures_user_time
    ON app.saved_fixtures (user_id, saved_at DESC);
