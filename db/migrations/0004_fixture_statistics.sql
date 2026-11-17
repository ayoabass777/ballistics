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
