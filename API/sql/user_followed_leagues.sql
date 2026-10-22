-- Requires app.app_users and the existing dim.dim_leagues metadata catalog.
CREATE TABLE IF NOT EXISTS app.user_followed_leagues (
    user_id TEXT NOT NULL REFERENCES app.app_users(user_id) ON DELETE CASCADE,
    league_id BIGINT NOT NULL REFERENCES dim.dim_leagues(league_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, league_id)
);
CREATE INDEX IF NOT EXISTS user_followed_leagues_league_idx
    ON app.user_followed_leagues (league_id);
