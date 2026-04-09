-- Bootstrap DDL for metadata/dimension tables used by ETL + dbt.
-- Idempotent and safe to rerun.

CREATE SCHEMA IF NOT EXISTS dim;

CREATE TABLE IF NOT EXISTS dim.dim_countries (
  country_id     BIGSERIAL    PRIMARY KEY,
  country_name   TEXT         NOT NULL UNIQUE,
  logo_file_id   UUID,
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

ALTER TABLE dim.dim_countries ADD COLUMN IF NOT EXISTS logo_file_id UUID;
ALTER TABLE dim.dim_countries ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE dim.dim_countries ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE UNIQUE INDEX IF NOT EXISTS dim_countries_country_name_uidx
  ON dim.dim_countries (country_name);

CREATE TABLE IF NOT EXISTS dim.dim_leagues (
  league_id      BIGSERIAL    PRIMARY KEY,
  country_id     BIGINT       NOT NULL REFERENCES dim.dim_countries(country_id),
  league_name    TEXT         NOT NULL,
  api_league_id  INT          NOT NULL UNIQUE,
  tier           INT,
  logo_file_id   UUID,
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

ALTER TABLE dim.dim_leagues ADD COLUMN IF NOT EXISTS tier INT;
ALTER TABLE dim.dim_leagues ADD COLUMN IF NOT EXISTS logo_file_id UUID;
ALTER TABLE dim.dim_leagues ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE dim.dim_leagues ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE UNIQUE INDEX IF NOT EXISTS dim_leagues_api_league_id_uidx
  ON dim.dim_leagues (api_league_id);

CREATE TABLE IF NOT EXISTS dim.dim_league_seasons (
  league_season_id         BIGSERIAL    PRIMARY KEY,
  league_id                BIGINT       NOT NULL REFERENCES dim.dim_leagues(league_id),
  season                   INT          NOT NULL,
  season_label             TEXT         NOT NULL,
  start_date               DATE,
  end_date                 DATE,
  fixtures_bootstrap_done  BOOLEAN      NOT NULL DEFAULT FALSE,
  fixtures_bootstrap_at    TIMESTAMPTZ,
  created_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),
  UNIQUE (league_id, season)
);

ALTER TABLE dim.dim_league_seasons ADD COLUMN IF NOT EXISTS fixtures_bootstrap_done BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE dim.dim_league_seasons ADD COLUMN IF NOT EXISTS fixtures_bootstrap_at TIMESTAMPTZ;
ALTER TABLE dim.dim_league_seasons ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE dim.dim_league_seasons ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE UNIQUE INDEX IF NOT EXISTS dim_league_seasons_league_id_season_uidx
  ON dim.dim_league_seasons (league_id, season);

CREATE TABLE IF NOT EXISTS dim.dim_venue (
  venue_id         BIGSERIAL    PRIMARY KEY,
  api_venue_id     INT          UNIQUE,
  venue_name       VARCHAR,
  venue_capacity   INT,
  venue_city       VARCHAR,
  logo_file_id     UUID,
  country_id       BIGINT REFERENCES dim.dim_countries(country_id),
  created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

ALTER TABLE dim.dim_venue ADD COLUMN IF NOT EXISTS api_venue_id INT;
ALTER TABLE dim.dim_venue ADD COLUMN IF NOT EXISTS venue_name VARCHAR;
ALTER TABLE dim.dim_venue ADD COLUMN IF NOT EXISTS venue_capacity INT;
ALTER TABLE dim.dim_venue ADD COLUMN IF NOT EXISTS venue_city VARCHAR;
ALTER TABLE dim.dim_venue ADD COLUMN IF NOT EXISTS logo_file_id UUID;
ALTER TABLE dim.dim_venue ADD COLUMN IF NOT EXISTS country_id BIGINT;
ALTER TABLE dim.dim_venue ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE dim.dim_venue ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE UNIQUE INDEX IF NOT EXISTS dim_venue_api_venue_id_uidx
  ON dim.dim_venue (api_venue_id);

CREATE TABLE IF NOT EXISTS dim.dim_teams (
  team_id       BIGSERIAL    PRIMARY KEY,
  api_team_id   INT          NOT NULL UNIQUE,
  team_name     TEXT         NOT NULL,
  team_code     VARCHAR(5),
  country_id    BIGINT       NOT NULL REFERENCES dim.dim_countries(country_id),
  logo_file_id  UUID,
  venue_id      BIGINT REFERENCES dim.dim_venue(venue_id),
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

ALTER TABLE dim.dim_teams ADD COLUMN IF NOT EXISTS logo_file_id UUID;
ALTER TABLE dim.dim_teams ADD COLUMN IF NOT EXISTS venue_id BIGINT;
ALTER TABLE dim.dim_teams ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE dim.dim_teams ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE UNIQUE INDEX IF NOT EXISTS dim_teams_api_team_id_uidx
  ON dim.dim_teams (api_team_id);

CREATE INDEX IF NOT EXISTS dim_leagues_country_id_idx
  ON dim.dim_leagues (country_id);

CREATE INDEX IF NOT EXISTS dim_league_seasons_league_id_idx
  ON dim.dim_league_seasons (league_id);

CREATE INDEX IF NOT EXISTS dim_league_seasons_bootstrap_done_idx
  ON dim.dim_league_seasons (fixtures_bootstrap_done);

CREATE INDEX IF NOT EXISTS dim_teams_country_id_idx
  ON dim.dim_teams (country_id);

CREATE INDEX IF NOT EXISTS dim_teams_venue_id_idx
  ON dim.dim_teams (venue_id);

CREATE INDEX IF NOT EXISTS dim_venue_country_id_idx
  ON dim.dim_venue (country_id);
