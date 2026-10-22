-- Apply after app.app_users exists. Does not copy Clerk profile or session data.
CREATE TABLE IF NOT EXISTS app.user_preferences (
    user_id TEXT PRIMARY KEY REFERENCES app.app_users(user_id) ON DELETE CASCADE,
    timezone_override TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
