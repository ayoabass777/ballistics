-- One-off for existing databases (new ones get this from the bootstrap SQL).
-- Standings keyed by group (conference / split round).
ALTER TABLE raw.raw_league_standings ADD COLUMN IF NOT EXISTS group_name TEXT NOT NULL DEFAULT '';
ALTER TABLE raw.raw_league_standings ADD COLUMN IF NOT EXISTS group_order INT;
ALTER TABLE raw.raw_league_standings ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE raw.raw_league_standings
  DROP CONSTRAINT IF EXISTS raw_league_standings_league_season_id_api_team_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS raw_league_standings_season_group_team_uidx
  ON raw.raw_league_standings (league_season_id, group_name, api_team_id);
