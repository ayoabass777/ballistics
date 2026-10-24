ALTER TABLE raw.raw_league_standings
  ADD COLUMN IF NOT EXISTS home_record JSONB,
  ADD COLUMN IF NOT EXISTS away_record JSONB;
