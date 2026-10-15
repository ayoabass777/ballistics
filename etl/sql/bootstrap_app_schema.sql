-- Bootstrap DDL for user accounts (D127).
-- Safe to run multiple times. Lives in its own `app` schema so dbt rebuilds
-- never touch it. league_id / fixture_id are provider IDs (docs/contracts/ids.md)
-- with no foreign keys into dbt tables; the API validates them.

CREATE SCHEMA IF NOT EXISTS app;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid() on PG < 13

-- Accounts: email + password only (no phone, no SMS).
CREATE TABLE IF NOT EXISTS app.users (
  user_id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  email               CITEXT       NOT NULL UNIQUE,
  password_hash       TEXT         NOT NULL,              -- argon2id
  email_verified_at   TIMESTAMPTZ,                        -- required before checkout
  timezone            TEXT         NOT NULL,              -- IANA, from the browser
  league_view         TEXT         NOT NULL DEFAULT 'followed'
                                   CHECK (league_view IN ('followed', 'all')),
  stripe_customer_id  TEXT         UNIQUE,
  onboarded_at        TIMESTAMPTZ,
  created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
  deleted_at          TIMESTAMPTZ
);

-- Login sessions. Only the SHA-256 of the cookie token is stored.
CREATE TABLE IF NOT EXISTS app.sessions (
  session_id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id      UUID         NOT NULL REFERENCES app.users ON DELETE CASCADE,
  token_hash   BYTEA        NOT NULL UNIQUE,
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
  expires_at   TIMESTAMPTZ  NOT NULL,
  revoked_at   TIMESTAMPTZ,
  user_agent   TEXT
);

CREATE INDEX IF NOT EXISTS app_sessions_user_idx ON app.sessions (user_id);

-- One-time email links: verify address, reset password. Single use, hashed.
CREATE TABLE IF NOT EXISTS app.email_tokens (
  token_id    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id     UUID         NOT NULL REFERENCES app.users ON DELETE CASCADE,
  purpose     TEXT         NOT NULL CHECK (purpose IN ('verify_email', 'reset_password')),
  token_hash  BYTEA        NOT NULL UNIQUE,
  expires_at  TIMESTAMPTZ  NOT NULL,              -- verify ~24h, reset ~1h
  used_at     TIMESTAMPTZ,
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS app_email_tokens_user_idx ON app.email_tokens (user_id, purpose);

-- Followed leagues (Discover and Matches scope). "At least one" rule lives in the API.
CREATE TABLE IF NOT EXISTS app.followed_leagues (
  user_id    UUID         NOT NULL REFERENCES app.users ON DELETE CASCADE,
  league_id  INT          NOT NULL,
  created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, league_id)
);

-- Saved: fixtures only for now.
CREATE TABLE IF NOT EXISTS app.saved_fixtures (
  user_id    UUID         NOT NULL REFERENCES app.users ON DELETE CASCADE,
  fixture_id INT          NOT NULL,
  saved_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, fixture_id)
);

-- Stripe subscription mirror. Written only by the webhook handler.
CREATE TABLE IF NOT EXISTS app.subscriptions (
  stripe_subscription_id  TEXT         PRIMARY KEY,
  user_id                 UUID         NOT NULL REFERENCES app.users ON DELETE CASCADE,
  stripe_price_id         TEXT         NOT NULL,
  status                  TEXT         NOT NULL,   -- Stripe value as sent
  current_period_end      TIMESTAMPTZ,
  cancel_at_period_end    BOOLEAN      NOT NULL DEFAULT false,
  created_at              TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at              TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS app_subscriptions_user_idx ON app.subscriptions (user_id);

-- Webhook idempotency: Stripe retries; each event is applied once.
CREATE TABLE IF NOT EXISTS app.stripe_events (
  event_id     TEXT         PRIMARY KEY,
  event_type   TEXT         NOT NULL,
  received_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  processed_at TIMESTAMPTZ
);

-- Single source of truth for the paywall.
CREATE OR REPLACE VIEW app.user_access AS
SELECT u.user_id,
       EXISTS (
         SELECT 1 FROM app.subscriptions s
         WHERE s.user_id = u.user_id
           AND s.status IN ('active', 'trialing')
           AND (s.current_period_end IS NULL OR s.current_period_end > now())
       ) AS has_access
FROM app.users u
WHERE u.deleted_at IS NULL;
