-- Apply after app.app_users exists. Does not copy Clerk profile or session data.
CREATE TABLE IF NOT EXISTS app.user_preferences (
    user_id TEXT PRIMARY KEY REFERENCES app.app_users(user_id) ON DELETE CASCADE,
    timezone_override TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- D164: beta usage events are on unless the member turns them off in Settings.
ALTER TABLE app.user_preferences ADD COLUMN IF NOT EXISTS usage_events_enabled BOOLEAN NOT NULL DEFAULT TRUE;
