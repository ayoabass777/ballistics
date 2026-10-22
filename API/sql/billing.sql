-- Stripe billing mirror and paywall (D143). Requires app.app_users (Clerk user IDs).
-- Ported from ballistics feat/user-accounts-db, now keyed on the Clerk user_id (TEXT).
-- Rows are written only by the Stripe webhook handler; the paywall reads app.user_access.

-- One Stripe customer per account.
CREATE TABLE IF NOT EXISTS app.billing_customers (
    user_id TEXT PRIMARY KEY REFERENCES app.app_users(user_id) ON DELETE CASCADE,
    stripe_customer_id TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Subscription mirror. Status is the Stripe value as sent.
CREATE TABLE IF NOT EXISTS app.subscriptions (
    stripe_subscription_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES app.app_users(user_id) ON DELETE CASCADE,
    stripe_price_id TEXT NOT NULL,
    status TEXT NOT NULL,
    current_period_end TIMESTAMPTZ,
    cancel_at_period_end BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS subscriptions_user_idx ON app.subscriptions (user_id);

-- Webhook idempotency: Stripe retries; each event is applied once.
CREATE TABLE IF NOT EXISTS app.stripe_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

-- Single source of truth for the paywall (app.app_users.plan is legacy and not read here).
CREATE OR REPLACE VIEW app.user_access AS
SELECT u.user_id,
       EXISTS (
           SELECT 1 FROM app.subscriptions s
           WHERE s.user_id = u.user_id
             AND s.status IN ('active', 'trialing')
             AND (s.current_period_end IS NULL OR s.current_period_end > NOW())
       ) AS has_access
FROM app.app_users u;
