-- Live data (combined 2026-11-18 from live-results-promotion #30 and
-- live-fulltime-stats #31, which each added a 0004; main's 0004 is
-- market_watch). Both parts are idempotent.

-- Part 1 of 2: live final results promoted to raw_fixtures (#30).
-- Live results feed raw.raw_fixtures (etl/src/live_results.py).
--
-- When the live poller sees a fixture reach a final status (FT, AET, PEN)
-- with both full-time goals, it lands the provider's by-id payload in S3
-- (incremental/live/...) and upserts it into raw.raw_fixtures through the
-- nightly loader (same row checks and quarantine). The nightly
-- fixture_update_dag then sweeps every promoted fixture: it re-fetches it by
-- id, applies the provider's current answer and records whether it differed.
--
-- These columns record what was promoted and what the sweep found.

ALTER TABLE raw.live_fixture_state
  ADD COLUMN IF NOT EXISTS promoted_at          TIMESTAMPTZ,  -- written to raw_fixtures from live
  ADD COLUMN IF NOT EXISTS promoted_status      TEXT,
  ADD COLUMN IF NOT EXISTS promoted_home_goals  INT,
  ADD COLUMN IF NOT EXISTS promoted_away_goals  INT,
  ADD COLUMN IF NOT EXISTS swept_at             TIMESTAMPTZ,  -- checked by the nightly sweep
  ADD COLUMN IF NOT EXISTS sweep_changed        BOOLEAN;      -- the sweep's answer differed

-- The nightly sweep's work list: promoted, not yet checked.
CREATE INDEX IF NOT EXISTS live_fixture_state_unswept_idx
  ON raw.live_fixture_state (promoted_at)
  WHERE promoted_at IS NOT NULL AND swept_at IS NULL;

-- Part 2 of 2: full-time match statistics (#31).
-- Full-time match statistics (etl/src/live_stats.py).
--
-- When a tracked fixture leaves the live list, the live poller fetches it by
-- id (fixtures?ids=); that response already carries the provider's match
-- statistics, so storing them costs no extra calls. Written once per
-- fixture at a final status (FT, AET, PEN); fixtures the poller never saw
-- live have no rows here.
--
-- One row per fixture, side and statistic, as the provider names it
-- (normalised to snake_case: corner_kicks, ball_possession, shots_on_goal,
-- yellow_cards, expected_goals, ...). The provider's set varies by league,
-- so new statistics need no schema change.

CREATE TABLE IF NOT EXISTS raw.fixture_statistics (
  fixture_id      BIGINT      NOT NULL,          -- raw.raw_fixtures.fixture_id
  api_fixture_id  BIGINT      NOT NULL,
  side            TEXT        NOT NULL CHECK (side IN ('home', 'away')),
  stat            TEXT        NOT NULL,          -- corner_kicks, ball_possession, ...
  value           NUMERIC,                       -- 57 for "57%"; NULL when the provider has none
  raw_value       TEXT,                          -- the provider's value as sent
  fixture_status  TEXT        NOT NULL,          -- status when captured (FT, AET, PEN)
  captured_at     TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (fixture_id, side, stat)
);

CREATE INDEX IF NOT EXISTS fixture_statistics_stat_idx
  ON raw.fixture_statistics (stat, fixture_id);
