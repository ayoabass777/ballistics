-- Bootstrap DDL for the fixture ETL raw table.
-- Safe to run multiple times in a fresh or existing environment.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.raw_fixtures (
  fixture_id                    BIGSERIAL    PRIMARY KEY,
  api_fixture_id                BIGINT       NOT NULL,
  api_league_id                 INT          NOT NULL,
  season                        INT          NOT NULL,
  kickoff_utc                   TIMESTAMPTZ  NOT NULL,
  fixture_status                TEXT         NOT NULL,
  home_team_id                  INT,
  home_team_name                TEXT,
  away_team_id                  INT,
  away_team_name                TEXT,
  home_team_halftime_goal       INT,
  away_team_halftime_goal       INT,
  home_team_fulltime_goal       INT,
  away_team_fulltime_goal       INT,
  home_fulltime_result          TEXT,
  away_fulltime_result          TEXT,
  home_halftime_result          TEXT,
  away_halftime_result          TEXT,
  created_at                    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS raw_raw_fixtures_fixture_id_idx
  ON raw.raw_fixtures (fixture_id);

CREATE INDEX IF NOT EXISTS raw_raw_fixtures_api_league_id_idx
  ON raw.raw_fixtures (api_league_id);

CREATE UNIQUE INDEX IF NOT EXISTS raw_raw_fixtures_api_fixture_id_uidx
  ON raw.raw_fixtures (api_fixture_id);

CREATE INDEX IF NOT EXISTS raw_raw_fixtures_kickoff_utc_idx
  ON raw.raw_fixtures (kickoff_utc);

-- Standings table used by ETL/API league pages.
CREATE TABLE IF NOT EXISTS raw.raw_league_standings (
  league_season_id              INT          NOT NULL,
  api_league_id                 INT          NOT NULL,
  season                        INT          NOT NULL,
  api_team_id                   INT          NOT NULL,
  team_name                     TEXT,
  rank                          INT,
  points                        INT,
  games_played                  INT,
  wins                          INT,
  draws                         INT,
  losses                        INT,
  goals_for                     INT,
  goals_against                 INT,
  goal_diff                     INT,
  form                          TEXT,
  created_at                    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  UNIQUE (league_season_id, api_team_id)
);

CREATE INDEX IF NOT EXISTS raw_raw_league_standings_league_idx
  ON raw.raw_league_standings (league_season_id);

CREATE INDEX IF NOT EXISTS raw_raw_league_standings_rank_idx
  ON raw.raw_league_standings (league_season_id, rank);

-- Elo table consumed by tier-adjusted dbt models.
CREATE SCHEMA IF NOT EXISTS elo;

CREATE TABLE IF NOT EXISTS elo.elo_rating (
  team_id                       INT               NOT NULL,
  league_season_id              INT               NOT NULL,
  fixture_id                    BIGINT            NOT NULL,
  pre_elo_rating                DOUBLE PRECISION,
  post_elo_rating               DOUBLE PRECISION,
  elo_change                    DOUBLE PRECISION,
  tier                          INT,
  snapshot_type                 TEXT              NOT NULL,
  created_at                    TIMESTAMPTZ       NOT NULL DEFAULT now(),
  PRIMARY KEY (team_id, league_season_id, fixture_id, snapshot_type)
);

CREATE INDEX IF NOT EXISTS elo_rating_team_fixture_idx
  ON elo.elo_rating (team_id, fixture_id);

CREATE INDEX IF NOT EXISTS elo_rating_league_fixture_idx
  ON elo.elo_rating (league_season_id, fixture_id);

-- Team form table consumed by API and fixture briefing.
CREATE TABLE IF NOT EXISTS public.team_form (
  league_season_id              INT               NOT NULL,
  league_name                   TEXT,
  event_name                    TEXT              NOT NULL,
  context                       TEXT              NOT NULL,
  team_id                       INT               NOT NULL,
  team_name                     TEXT,
  next_opponent_id              INT,
  next_opponent_name            TEXT,
  next_kickoff_utc              TIMESTAMPTZ,
  score                         DOUBLE PRECISION,
  n                             INT,
  k                             INT,
  misses                        INT,
  p_bayes                       DOUBLE PRECISION,
  streak_length                 INT,
  updated_at                    TIMESTAMPTZ       NOT NULL DEFAULT now(),
  PRIMARY KEY (league_season_id, event_name, context, team_id)
);

CREATE INDEX IF NOT EXISTS team_form_team_context_idx
  ON public.team_form (team_id, context);

CREATE INDEX IF NOT EXISTS team_form_league_event_idx
  ON public.team_form (league_season_id, event_name, context);
