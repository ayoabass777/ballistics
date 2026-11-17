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
