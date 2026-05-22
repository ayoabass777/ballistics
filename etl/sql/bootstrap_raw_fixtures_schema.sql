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

-- Data detector tables used to track pipeline flow through S3, raw tables, and dbt.
CREATE SCHEMA IF NOT EXISTS data_detector;

CREATE TABLE IF NOT EXISTS data_detector.pipeline_runs (
  pipeline_run_id              BIGSERIAL    PRIMARY KEY,
  run_id                       TEXT         NOT NULL,
  dag_id                       TEXT         NOT NULL,
  task_id                      TEXT,
  run_type                     TEXT,
  status                       TEXT         NOT NULL,
  started_at                   TIMESTAMPTZ,
  finished_at                  TIMESTAMPTZ,
  kafka_started_at             TIMESTAMPTZ,
  kafka_finished_at            TIMESTAMPTZ,
  kafka_topic                  TEXT,
  kafka_partition              INT,
  kafka_start_offset           BIGINT,
  kafka_finish_offset          BIGINT,
  error_message                TEXT,
  metadata                     JSONB        NOT NULL DEFAULT '{}'::jsonb,
  created_at                   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  UNIQUE (dag_id, task_id, run_id)
);

ALTER TABLE data_detector.pipeline_runs ADD COLUMN IF NOT EXISTS kafka_started_at TIMESTAMPTZ;
ALTER TABLE data_detector.pipeline_runs ADD COLUMN IF NOT EXISTS kafka_finished_at TIMESTAMPTZ;
ALTER TABLE data_detector.pipeline_runs ADD COLUMN IF NOT EXISTS kafka_topic TEXT;
ALTER TABLE data_detector.pipeline_runs ADD COLUMN IF NOT EXISTS kafka_partition INT;
ALTER TABLE data_detector.pipeline_runs ADD COLUMN IF NOT EXISTS kafka_start_offset BIGINT;
ALTER TABLE data_detector.pipeline_runs ADD COLUMN IF NOT EXISTS kafka_finish_offset BIGINT;

CREATE INDEX IF NOT EXISTS data_detector_pipeline_runs_status_idx
  ON data_detector.pipeline_runs (status);

CREATE INDEX IF NOT EXISTS data_detector_pipeline_runs_dag_started_idx
  ON data_detector.pipeline_runs (dag_id, started_at DESC);

CREATE TABLE IF NOT EXISTS data_detector.data_movements (
  data_movement_id             BIGSERIAL    PRIMARY KEY,
  pipeline_run_id              BIGINT       REFERENCES data_detector.pipeline_runs(pipeline_run_id),
  run_id                       TEXT,
  dag_id                       TEXT,
  task_id                      TEXT,
  movement_type                TEXT         NOT NULL,
  source_system                TEXT         NOT NULL,
  source_name                  TEXT,
  source_s3_key                TEXT,
  target_schema                TEXT,
  target_table                 TEXT,
  row_count                    BIGINT,
  inserted_count               BIGINT,
  updated_count                BIGINT,
  deleted_count                BIGINT,
  failed_count                 BIGINT       NOT NULL DEFAULT 0,
  watermark_column             TEXT,
  watermark_min                TIMESTAMPTZ,
  watermark_max                TIMESTAMPTZ,
  status                       TEXT         NOT NULL,
  details                      JSONB        NOT NULL DEFAULT '{}'::jsonb,
  detected_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS data_detector_data_movements_target_idx
  ON data_detector.data_movements (target_schema, target_table, detected_at DESC);

CREATE INDEX IF NOT EXISTS data_detector_data_movements_status_idx
  ON data_detector.data_movements (status, detected_at DESC);

CREATE TABLE IF NOT EXISTS data_detector.table_snapshots (
  table_snapshot_id            BIGSERIAL    PRIMARY KEY,
  pipeline_run_id              BIGINT       REFERENCES data_detector.pipeline_runs(pipeline_run_id),
  run_id                       TEXT,
  dag_id                       TEXT,
  table_schema                 TEXT         NOT NULL,
  table_name                   TEXT         NOT NULL,
  row_count                    BIGINT       NOT NULL,
  max_created_at               TIMESTAMPTZ,
  max_updated_at               TIMESTAMPTZ,
  max_kickoff_utc              TIMESTAMPTZ,
  checksum                     TEXT,
  details                      JSONB        NOT NULL DEFAULT '{}'::jsonb,
  snapshot_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS data_detector_table_snapshots_table_idx
  ON data_detector.table_snapshots (table_schema, table_name, snapshot_at DESC);

CREATE TABLE IF NOT EXISTS data_detector.data_quality_events (
  data_quality_event_id        BIGSERIAL    PRIMARY KEY,
  pipeline_run_id              BIGINT       REFERENCES data_detector.pipeline_runs(pipeline_run_id),
  run_id                       TEXT,
  dag_id                       TEXT,
  task_id                      TEXT,
  severity                     TEXT         NOT NULL,
  check_name                   TEXT         NOT NULL,
  table_schema                 TEXT,
  table_name                   TEXT,
  source_s3_key                TEXT,
  status                       TEXT         NOT NULL,
  details                      JSONB        NOT NULL DEFAULT '{}'::jsonb,
  error_message                TEXT,
  detected_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS data_detector_quality_events_severity_idx
  ON data_detector.data_quality_events (severity, detected_at DESC);

CREATE INDEX IF NOT EXISTS data_detector_quality_events_table_idx
  ON data_detector.data_quality_events (table_schema, table_name, detected_at DESC);
