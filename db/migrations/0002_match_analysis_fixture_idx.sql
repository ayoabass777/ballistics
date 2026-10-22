-- Match details shows a fixture's most recent forecast, which after kickoff
-- comes from an earlier run than the latest (runs cover not-started fixtures
-- only; D152). The primary key (run_ds, fixture_id) cannot serve a lookup by
-- fixture across runs.
CREATE INDEX IF NOT EXISTS match_analysis_fixture_idx
    ON raw.match_analysis (fixture_id, run_ds DESC);
