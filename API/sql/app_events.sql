-- Beta usage events (D164). Run after app.app_users exists.
-- Identity comes from the Clerk token; props never hold email, name or free text.
CREATE TABLE IF NOT EXISTS app.app_events (
    event_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES app.app_users(user_id) ON DELETE CASCADE,
    session_id UUID NOT NULL,
    event_name TEXT NOT NULL,
    props JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS app_events_user_time_idx
    ON app.app_events (user_id, occurred_at);
