-- One-off for existing databases (new ones get this from the bootstrap SQL).
-- Competition round from API-Football league.round ("Regular Season - 12",
-- "Play-offs", "Final"). Past seasons are filled by etl.src.backfill_fixture_rounds.
ALTER TABLE raw.raw_fixtures ADD COLUMN IF NOT EXISTS round TEXT;
