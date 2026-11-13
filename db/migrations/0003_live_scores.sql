-- Live scores (docs/live_scores.md, phase 1). Written only by the live
-- poller (etl/src/live_poller.py); never by the nightly pipeline, and the
-- poller never writes raw.raw_fixtures, which stays the source of truth for
-- results.

-- One row per tracked fixture the poller has seen live. Kept after full time
-- so the page can show the final score before the nightly update.
CREATE TABLE IF NOT EXISTS raw.live_fixture_state (
  fixture_id        BIGINT      PRIMARY KEY,   -- raw.raw_fixtures.fixture_id
  api_fixture_id    BIGINT      NOT NULL UNIQUE,
  status            TEXT        NOT NULL,      -- provider short status: 1H, HT, 2H, FT…
  elapsed           INT,                       -- minute
  extra             INT,                       -- stoppage minutes
  home_goals        INT,
  away_goals        INT,
  home_red_cards    INT         NOT NULL DEFAULT 0,
  away_red_cards    INT         NOT NULL DEFAULT 0,
  polled_at         TIMESTAMPTZ NOT NULL,      -- last poll that returned this fixture
  changed_at        TIMESTAMPTZ NOT NULL,      -- last poll that changed it
  finished_at       TIMESTAMPTZ                -- when a final status was first seen
);

CREATE INDEX IF NOT EXISTS live_fixture_state_polled_idx
  ON raw.live_fixture_state (polled_at DESC);

-- Goals, cards, substitutions and VAR decisions, appended as they appear.
-- The provider has no event id, so event_key is built from the event's own
-- fields. An event the provider stops sending is marked revoked, never
-- deleted. Revoked only means "removed by the provider" (often a phantom
-- entry); VAR decisions are the provider's own type = 'Var' events.
CREATE TABLE IF NOT EXISTS raw.live_events (
  event_id          BIGSERIAL   PRIMARY KEY,
  fixture_id        BIGINT      NOT NULL,
  event_key         TEXT        NOT NULL,
  minute            INT,
  extra             INT,
  side              TEXT        CHECK (side IN ('home', 'away')),
  type              TEXT        NOT NULL,      -- Goal, Card, subst, Var
  detail            TEXT,                      -- Normal Goal, Own Goal, Penalty, Red Card…
  player            TEXT,
  assist            TEXT,
  seen_at           TIMESTAMPTZ NOT NULL,
  revoked_at        TIMESTAMPTZ,
  UNIQUE (fixture_id, event_key)
);

CREATE INDEX IF NOT EXISTS live_events_fixture_idx
  ON raw.live_events (fixture_id, minute, event_id);
