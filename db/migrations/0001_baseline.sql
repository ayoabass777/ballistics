-- 0001 baseline: pipeline-owned schema as of 2026-10-22.
--
-- Generated from the local database (PostgreSQL 17.4) with
--   pg_dump --schema-only --no-owner --no-privileges --no-comments
--     -n raw -n dim -n elo -n forecast -n forecast_archive -n data_detector
--     -T <dbt models materialised in raw>
-- Covers tables, views, functions and triggers created by etl/sql, the
-- dated migrations_*.sql and apply_*.sh scripts, and the tables Python code
-- creates on first use. Excluded, because other tools own them:
--   * dbt models (raw_stg/raw_int/raw_mart/raw_config, and in raw:
--     engine_fixtures, engine_head_to_head, engine_head_to_head_events,
--     fact_fixture_xg, fact_fixtures, fact_head_to_head,
--     fact_head_to_head_events, fact_head_to_head_latest,
--     fact_head_to_head_streaks) -- built by `dbt build`.
--   * app schema -- football_stats_project/API/sql/bootstrap_accounts.sql,
--     run after this (app.user_followed_leagues references dim.dim_leagues).
--   * Airflow metadata -- its own database on staging.
-- Existing databases (local, or restored from a dump) record this file with
-- `python scripts/migrate.py baseline 0001` instead of running it.

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;  -- used by raw_mart search index

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: data_detector; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA data_detector;


--
-- Name: dim; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA dim;


--
-- Name: elo; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA elo;


--
-- Name: forecast; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA forecast;


--
-- Name: forecast_archive; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA forecast_archive;


--
-- Name: raw; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA raw;


--
-- Name: reject_mutation(); Type: FUNCTION; Schema: forecast; Owner: -
--

CREATE FUNCTION forecast.reject_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN
    RAISE EXCEPTION 'forecast history is append-only';
END $$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: daily_flow_checks; Type: TABLE; Schema: data_detector; Owner: -
--

CREATE TABLE data_detector.daily_flow_checks (
    check_date date NOT NULL,
    dataset_id text NOT NULL,
    layer text NOT NULL,
    status text NOT NULL,
    blocked_by text[] DEFAULT '{}'::text[] NOT NULL,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    checked_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: data_movements; Type: TABLE; Schema: data_detector; Owner: -
--

CREATE TABLE data_detector.data_movements (
    data_movement_id bigint NOT NULL,
    pipeline_run_id bigint,
    run_id text,
    dag_id text,
    task_id text,
    movement_type text NOT NULL,
    source_system text NOT NULL,
    source_name text,
    source_s3_key text,
    target_schema text,
    target_table text,
    row_count bigint,
    inserted_count bigint,
    updated_count bigint,
    deleted_count bigint,
    failed_count bigint DEFAULT 0 NOT NULL,
    watermark_column text,
    watermark_min timestamp with time zone,
    watermark_max timestamp with time zone,
    status text NOT NULL,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    detected_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: data_movements_data_movement_id_seq; Type: SEQUENCE; Schema: data_detector; Owner: -
--

CREATE SEQUENCE data_detector.data_movements_data_movement_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: data_movements_data_movement_id_seq; Type: SEQUENCE OWNED BY; Schema: data_detector; Owner: -
--

ALTER SEQUENCE data_detector.data_movements_data_movement_id_seq OWNED BY data_detector.data_movements.data_movement_id;


--
-- Name: data_quality_events; Type: TABLE; Schema: data_detector; Owner: -
--

CREATE TABLE data_detector.data_quality_events (
    data_quality_event_id bigint NOT NULL,
    pipeline_run_id bigint,
    run_id text,
    dag_id text,
    task_id text,
    severity text NOT NULL,
    check_name text NOT NULL,
    table_schema text,
    table_name text,
    source_s3_key text,
    status text NOT NULL,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    detected_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: data_quality_events_data_quality_event_id_seq; Type: SEQUENCE; Schema: data_detector; Owner: -
--

CREATE SEQUENCE data_detector.data_quality_events_data_quality_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: data_quality_events_data_quality_event_id_seq; Type: SEQUENCE OWNED BY; Schema: data_detector; Owner: -
--

ALTER SEQUENCE data_detector.data_quality_events_data_quality_event_id_seq OWNED BY data_detector.data_quality_events.data_quality_event_id;


--
-- Name: dataset_dependencies; Type: TABLE; Schema: data_detector; Owner: -
--

CREATE TABLE data_detector.dataset_dependencies (
    dataset_id text NOT NULL,
    upstream_id text NOT NULL
);


--
-- Name: datasets; Type: TABLE; Schema: data_detector; Owner: -
--

CREATE TABLE data_detector.datasets (
    dataset_id text NOT NULL,
    layer text NOT NULL,
    description text,
    producer_dag text,
    producer_task text,
    check_spec jsonb NOT NULL,
    synced_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: pipeline_runs; Type: TABLE; Schema: data_detector; Owner: -
--

CREATE TABLE data_detector.pipeline_runs (
    pipeline_run_id bigint NOT NULL,
    run_id text NOT NULL,
    dag_id text NOT NULL,
    task_id text,
    run_type text,
    status text NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    kafka_started_at timestamp with time zone,
    kafka_finished_at timestamp with time zone,
    kafka_topic text,
    kafka_partition integer,
    kafka_start_offset bigint,
    kafka_finish_offset bigint,
    error_message text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: pipeline_runs_pipeline_run_id_seq; Type: SEQUENCE; Schema: data_detector; Owner: -
--

CREATE SEQUENCE data_detector.pipeline_runs_pipeline_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pipeline_runs_pipeline_run_id_seq; Type: SEQUENCE OWNED BY; Schema: data_detector; Owner: -
--

ALTER SEQUENCE data_detector.pipeline_runs_pipeline_run_id_seq OWNED BY data_detector.pipeline_runs.pipeline_run_id;


--
-- Name: table_snapshots; Type: TABLE; Schema: data_detector; Owner: -
--

CREATE TABLE data_detector.table_snapshots (
    table_snapshot_id bigint NOT NULL,
    pipeline_run_id bigint,
    run_id text,
    dag_id text,
    table_schema text NOT NULL,
    table_name text NOT NULL,
    row_count bigint NOT NULL,
    max_created_at timestamp with time zone,
    max_updated_at timestamp with time zone,
    max_kickoff_utc timestamp with time zone,
    checksum text,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    snapshot_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: table_snapshots_table_snapshot_id_seq; Type: SEQUENCE; Schema: data_detector; Owner: -
--

CREATE SEQUENCE data_detector.table_snapshots_table_snapshot_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: table_snapshots_table_snapshot_id_seq; Type: SEQUENCE OWNED BY; Schema: data_detector; Owner: -
--

ALTER SEQUENCE data_detector.table_snapshots_table_snapshot_id_seq OWNED BY data_detector.table_snapshots.table_snapshot_id;


--
-- Name: dim_countries; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.dim_countries (
    country_id bigint NOT NULL,
    country_name text NOT NULL,
    logo_file_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: dim_countries_country_id_seq; Type: SEQUENCE; Schema: dim; Owner: -
--

CREATE SEQUENCE dim.dim_countries_country_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_countries_country_id_seq; Type: SEQUENCE OWNED BY; Schema: dim; Owner: -
--

ALTER SEQUENCE dim.dim_countries_country_id_seq OWNED BY dim.dim_countries.country_id;


--
-- Name: dim_league_seasons; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.dim_league_seasons (
    league_season_id bigint NOT NULL,
    league_id bigint NOT NULL,
    season integer NOT NULL,
    season_label text NOT NULL,
    start_date date,
    end_date date,
    fixtures_bootstrap_done boolean DEFAULT false NOT NULL,
    fixtures_bootstrap_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: dim_league_seasons_league_season_id_seq; Type: SEQUENCE; Schema: dim; Owner: -
--

CREATE SEQUENCE dim.dim_league_seasons_league_season_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_league_seasons_league_season_id_seq; Type: SEQUENCE OWNED BY; Schema: dim; Owner: -
--

ALTER SEQUENCE dim.dim_league_seasons_league_season_id_seq OWNED BY dim.dim_league_seasons.league_season_id;


--
-- Name: dim_leagues; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.dim_leagues (
    league_id bigint NOT NULL,
    country_id bigint NOT NULL,
    league_name text NOT NULL,
    api_league_id integer NOT NULL,
    tier integer,
    logo_file_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: dim_leagues_league_id_seq; Type: SEQUENCE; Schema: dim; Owner: -
--

CREATE SEQUENCE dim.dim_leagues_league_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_leagues_league_id_seq; Type: SEQUENCE OWNED BY; Schema: dim; Owner: -
--

ALTER SEQUENCE dim.dim_leagues_league_id_seq OWNED BY dim.dim_leagues.league_id;


--
-- Name: dim_teams; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.dim_teams (
    team_id bigint NOT NULL,
    api_team_id integer NOT NULL,
    team_name text NOT NULL,
    team_code character varying(5),
    country_id bigint NOT NULL,
    logo_file_id uuid,
    venue_id bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: dim_teams_team_id_seq; Type: SEQUENCE; Schema: dim; Owner: -
--

CREATE SEQUENCE dim.dim_teams_team_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_teams_team_id_seq; Type: SEQUENCE OWNED BY; Schema: dim; Owner: -
--

ALTER SEQUENCE dim.dim_teams_team_id_seq OWNED BY dim.dim_teams.team_id;


--
-- Name: dim_venue; Type: TABLE; Schema: dim; Owner: -
--

CREATE TABLE dim.dim_venue (
    venue_id bigint NOT NULL,
    api_venue_id integer,
    venue_name character varying,
    venue_capacity integer,
    venue_city character varying,
    logo_file_id uuid,
    country_id bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: dim_venue_venue_id_seq; Type: SEQUENCE; Schema: dim; Owner: -
--

CREATE SEQUENCE dim.dim_venue_venue_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_venue_venue_id_seq; Type: SEQUENCE OWNED BY; Schema: dim; Owner: -
--

ALTER SEQUENCE dim.dim_venue_venue_id_seq OWNED BY dim.dim_venue.venue_id;


--
-- Name: elo_rating; Type: TABLE; Schema: elo; Owner: -
--

CREATE TABLE elo.elo_rating (
    team_id integer NOT NULL,
    league_season_id integer NOT NULL,
    fixture_id bigint NOT NULL,
    pre_elo_rating double precision,
    post_elo_rating double precision,
    elo_change double precision,
    tier integer,
    snapshot_type text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: team_elo_history; Type: TABLE; Schema: elo; Owner: -
--

CREATE TABLE elo.team_elo_history (
    team_id integer NOT NULL,
    fixture_id integer NOT NULL,
    kickoff_utc timestamp with time zone NOT NULL,
    rating_before double precision NOT NULL,
    rating_after double precision NOT NULL,
    expected_score double precision NOT NULL,
    actual_score double precision NOT NULL,
    k_value integer NOT NULL,
    season text,
    league_name text,
    computed_at timestamp with time zone DEFAULT now() NOT NULL,
    rating_before_flat double precision,
    rating_after_flat double precision
);


--
-- Name: entries; Type: TABLE; Schema: forecast; Owner: -
--

CREATE TABLE forecast.entries (
    id bigint NOT NULL,
    snapshot_id bigint NOT NULL,
    fixture_id bigint NOT NULL,
    event_key text NOT NULL,
    probability double precision NOT NULL,
    kickoff timestamp with time zone NOT NULL,
    eligible boolean NOT NULL,
    eligibility_reason text NOT NULL,
    policy text NOT NULL,
    detail jsonb NOT NULL,
    CONSTRAINT entries_probability_check CHECK (((probability >= (0)::double precision) AND (probability <= (1)::double precision)))
);


--
-- Name: entries_id_seq; Type: SEQUENCE; Schema: forecast; Owner: -
--

CREATE SEQUENCE forecast.entries_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: entries_id_seq; Type: SEQUENCE OWNED BY; Schema: forecast; Owner: -
--

ALTER SEQUENCE forecast.entries_id_seq OWNED BY forecast.entries.id;


--
-- Name: snapshots; Type: TABLE; Schema: forecast; Owner: -
--

CREATE TABLE forecast.snapshots (
    id bigint NOT NULL,
    content_hash text NOT NULL,
    run_ds date NOT NULL,
    recorded_at timestamp with time zone NOT NULL,
    payload jsonb NOT NULL,
    provenance jsonb NOT NULL
);


--
-- Name: first_forecasts; Type: VIEW; Schema: forecast; Owner: -
--

CREATE VIEW forecast.first_forecasts AS
 SELECT DISTINCT ON (e.fixture_id, e.event_key, e.policy) e.id,
    e.snapshot_id,
    e.fixture_id,
    e.event_key,
    e.probability,
    e.kickoff,
    e.eligible,
    e.eligibility_reason,
    e.policy,
    e.detail,
    s.recorded_at,
    s.run_ds
   FROM (forecast.entries e
     JOIN forecast.snapshots s ON ((s.id = e.snapshot_id)))
  WHERE e.eligible
  ORDER BY e.fixture_id, e.event_key, e.policy, s.recorded_at, s.id, e.id;


--
-- Name: latest_forecasts; Type: VIEW; Schema: forecast; Owner: -
--

CREATE VIEW forecast.latest_forecasts AS
 SELECT DISTINCT ON (e.fixture_id, e.event_key, e.policy) e.id,
    e.snapshot_id,
    e.fixture_id,
    e.event_key,
    e.probability,
    e.kickoff,
    e.eligible,
    e.eligibility_reason,
    e.policy,
    e.detail,
    s.recorded_at,
    s.run_ds
   FROM (forecast.entries e
     JOIN forecast.snapshots s ON ((s.id = e.snapshot_id)))
  WHERE e.eligible
  ORDER BY e.fixture_id, e.event_key, e.policy, s.recorded_at DESC, s.id DESC, e.id DESC;


--
-- Name: settlements; Type: TABLE; Schema: forecast; Owner: -
--

CREATE TABLE forecast.settlements (
    id bigint NOT NULL,
    entry_id bigint NOT NULL,
    observed_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    grader text NOT NULL,
    status text NOT NULL,
    reason text NOT NULL,
    evidence jsonb NOT NULL,
    evidence_hash text NOT NULL,
    CONSTRAINT settlements_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'hit'::text, 'miss'::text, 'void'::text, 'unavailable'::text, 'excluded'::text])))
);


--
-- Name: settlements_id_seq; Type: SEQUENCE; Schema: forecast; Owner: -
--

CREATE SEQUENCE forecast.settlements_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: settlements_id_seq; Type: SEQUENCE OWNED BY; Schema: forecast; Owner: -
--

ALTER SEQUENCE forecast.settlements_id_seq OWNED BY forecast.settlements.id;


--
-- Name: snapshots_id_seq; Type: SEQUENCE; Schema: forecast; Owner: -
--

CREATE SEQUENCE forecast.snapshots_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: forecast; Owner: -
--

ALTER SEQUENCE forecast.snapshots_id_seq OWNED BY forecast.snapshots.id;


--
-- Name: batches; Type: TABLE; Schema: forecast_archive; Owner: -
--

CREATE TABLE forecast_archive.batches (
    bucket text NOT NULL,
    source_table text NOT NULL,
    first_id bigint NOT NULL,
    last_id bigint NOT NULL,
    row_count integer NOT NULL,
    object_key text NOT NULL,
    sha256 text NOT NULL,
    manifest_key text NOT NULL,
    verified_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
);


--
-- Name: checkpoints; Type: TABLE; Schema: forecast_archive; Owner: -
--

CREATE TABLE forecast_archive.checkpoints (
    bucket text NOT NULL,
    source_table text NOT NULL,
    last_id bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
);


--
-- Name: raw_league_standings; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_league_standings (
    league_season_id integer NOT NULL,
    api_league_id integer NOT NULL,
    season integer NOT NULL,
    api_team_id integer NOT NULL,
    team_name text,
    rank integer,
    points integer,
    games_played integer,
    wins integer,
    draws integer,
    losses integer,
    goals_for integer,
    goals_against integer,
    goal_diff integer,
    form text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    group_name text DEFAULT ''::text NOT NULL,
    group_order integer,
    description text
);


--
-- Name: big_dog_watch; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.big_dog_watch (
    fixture_id bigint NOT NULL,
    first_run_ds date NOT NULL,
    last_run_ds date NOT NULL,
    kickoff timestamp with time zone NOT NULL,
    league text,
    country text,
    favourite text NOT NULL,
    underdog text,
    fav_is_home boolean NOT NULL,
    elo_gap numeric NOT NULL,
    dc_win_prob numeric,
    dc_confirms boolean,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: card_versions; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.card_versions (
    id bigint NOT NULL,
    fixture_id bigint NOT NULL,
    context_hash text NOT NULL,
    content_hash text NOT NULL,
    version integer NOT NULL,
    card jsonb NOT NULL,
    run_ds date NOT NULL,
    recorded_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
);


--
-- Name: card_versions_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

CREATE SEQUENCE raw.card_versions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: card_versions_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: -
--

ALTER SEQUENCE raw.card_versions_id_seq OWNED BY raw.card_versions.id;


--
-- Name: consensus_pool; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.consensus_pool (
    run_ds date NOT NULL,
    fixture_id bigint NOT NULL,
    unified_market text NOT NULL,
    sportybet_market text,
    sportybet_selection text,
    line numeric,
    n_models integer,
    models text[],
    avg_confidence numeric,
    max_confidence numeric,
    consensus_score numeric,
    est_odds numeric,
    real_odds numeric,
    odds numeric,
    ev numeric,
    has_odds boolean,
    market_tier text,
    strength_rank jsonb,
    league text,
    country text,
    kickoff timestamp with time zone,
    generated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: engine_runs; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.engine_runs (
    run_ds date NOT NULL,
    status text NOT NULL,
    n_fixtures integer DEFAULT 0 NOT NULL,
    generated_at timestamp with time zone,
    finished_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: fixture_xg; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.fixture_xg (
    fixture_id bigint NOT NULL,
    api_fixture_id bigint,
    home_xg numeric,
    away_xg numeric,
    captured_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: fixture_xg_attempts; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.fixture_xg_attempts (
    fixture_id bigint NOT NULL,
    api_fixture_id bigint,
    status text NOT NULL,
    attempted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT fixture_xg_attempts_status_check CHECK ((status = ANY (ARRAY['available'::text, 'missing'::text, 'error'::text])))
);


--
-- Name: match_analysis; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.match_analysis (
    run_ds date NOT NULL,
    fixture_id bigint NOT NULL,
    payload jsonb NOT NULL,
    written_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: model_analysis; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.model_analysis (
    run_ds date NOT NULL,
    model text NOT NULL,
    fixture_id bigint NOT NULL,
    payload jsonb NOT NULL,
    written_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: model_picks; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.model_picks (
    id bigint NOT NULL,
    run_ds date NOT NULL,
    model text NOT NULL,
    fixture_id bigint NOT NULL,
    unified_market text NOT NULL,
    probability numeric,
    raw_probability numeric,
    detail jsonb,
    written_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: model_picks_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

CREATE SEQUENCE raw.model_picks_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: model_picks_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: -
--

ALTER SEQUENCE raw.model_picks_id_seq OWNED BY raw.model_picks.id;


--
-- Name: model_processing_state; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.model_processing_state (
    scope text NOT NULL,
    entity_key text NOT NULL,
    input_hash text NOT NULL,
    result_id bigint NOT NULL,
    processed_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
);


--
-- Name: model_result_versions; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.model_result_versions (
    id bigint NOT NULL,
    scope text NOT NULL,
    entity_key text NOT NULL,
    output_hash text NOT NULL,
    output jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
);


--
-- Name: model_result_versions_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

CREATE SEQUENCE raw.model_result_versions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: model_result_versions_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: -
--

ALTER SEQUENCE raw.model_result_versions_id_seq OWNED BY raw.model_result_versions.id;


--
-- Name: model_runs; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.model_runs (
    run_ds date NOT NULL,
    model text NOT NULL,
    status text NOT NULL,
    n_picks integer DEFAULT 0 NOT NULL,
    finished_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: raw_fixtures; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_fixtures (
    fixture_id bigint NOT NULL,
    api_fixture_id bigint NOT NULL,
    api_league_id integer NOT NULL,
    season integer NOT NULL,
    kickoff_utc timestamp with time zone NOT NULL,
    fixture_status text NOT NULL,
    home_team_id integer,
    home_team_name text,
    away_team_id integer,
    away_team_name text,
    home_team_halftime_goal integer,
    away_team_halftime_goal integer,
    home_team_fulltime_goal integer,
    away_team_fulltime_goal integer,
    home_fulltime_result text,
    away_fulltime_result text,
    home_halftime_result text,
    away_halftime_result text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    round text
);


--
-- Name: raw_fixtures_fixture_id_seq; Type: SEQUENCE; Schema: raw; Owner: -
--

CREATE SEQUENCE raw.raw_fixtures_fixture_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_fixtures_fixture_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: -
--

ALTER SEQUENCE raw.raw_fixtures_fixture_id_seq OWNED BY raw.raw_fixtures.fixture_id;


--
-- Name: raw_league_standings_status; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.raw_league_standings_status (
    league_season_id integer NOT NULL,
    api_league_id integer NOT NULL,
    season integer NOT NULL,
    status text NOT NULL,
    unprocessed_rows integer DEFAULT 0 NOT NULL,
    unprocessed_teams text[],
    reason text,
    unprocessed_since timestamp with time zone,
    replay_attempts integer DEFAULT 0 NOT NULL,
    dlq_key text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: sportybet_odds; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.sportybet_odds (
    fixture_id bigint NOT NULL,
    sporty_event_id text NOT NULL,
    market text NOT NULL,
    line text NOT NULL,
    selection text NOT NULL,
    decimal_odds numeric NOT NULL,
    match_score numeric,
    captured_at timestamp with time zone NOT NULL
);


--
-- Name: xg_league_coverage; Type: VIEW; Schema: raw; Owner: -
--

CREATE VIEW raw.xg_league_coverage AS
 WITH completed AS (
         SELECT raw_fixtures.api_league_id,
            raw_fixtures.season,
            count(*) AS completed_fixtures,
            min(raw_fixtures.kickoff_utc) AS first_completed_kickoff,
            max(raw_fixtures.kickoff_utc) AS last_completed_kickoff
           FROM raw.raw_fixtures
          WHERE (raw_fixtures.fixture_status = 'FT'::text)
          GROUP BY raw_fixtures.api_league_id, raw_fixtures.season
        ), covered AS (
         SELECT rf.api_league_id,
            rf.season,
            count(x_1.fixture_id) AS xg_fixtures,
            count(x_1.fixture_id) FILTER (WHERE ((x_1.home_xg IS NOT NULL) AND (x_1.away_xg IS NOT NULL))) AS complete_xg_fixtures,
            max(x_1.captured_at) AS last_xg_captured_at
           FROM (raw.raw_fixtures rf
             LEFT JOIN raw.fixture_xg x_1 ON ((x_1.fixture_id = rf.fixture_id)))
          WHERE (rf.fixture_status = 'FT'::text)
          GROUP BY rf.api_league_id, rf.season
        ), attempts AS (
         SELECT rf.api_league_id,
            rf.season,
            count(*) FILTER (WHERE (a_1.status = 'missing'::text)) AS missing_xg_attempts,
            count(*) FILTER (WHERE (a_1.status = 'error'::text)) AS error_xg_attempts,
            max(a_1.attempted_at) AS last_xg_attempt_at
           FROM (raw.raw_fixtures rf
             JOIN raw.fixture_xg_attempts a_1 ON ((a_1.fixture_id = rf.fixture_id)))
          GROUP BY rf.api_league_id, rf.season
        )
 SELECT c.api_league_id,
    c.season,
    c.completed_fixtures,
    COALESCE(x.xg_fixtures, (0)::bigint) AS xg_fixtures,
    COALESCE(x.complete_xg_fixtures, (0)::bigint) AS complete_xg_fixtures,
    round(((100.0 * (COALESCE(x.complete_xg_fixtures, (0)::bigint))::numeric) / (NULLIF(c.completed_fixtures, 0))::numeric), 2) AS coverage_pct,
    c.first_completed_kickoff,
    c.last_completed_kickoff,
    x.last_xg_captured_at,
    COALESCE(a.missing_xg_attempts, (0)::bigint) AS missing_xg_attempts,
    COALESCE(a.error_xg_attempts, (0)::bigint) AS error_xg_attempts,
    a.last_xg_attempt_at
   FROM ((completed c
     LEFT JOIN covered x ON (((x.api_league_id = c.api_league_id) AND (x.season = c.season))))
     LEFT JOIN attempts a ON (((a.api_league_id = c.api_league_id) AND (a.season = c.season))));


--
-- Name: xg_league_probes; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.xg_league_probes (
    api_league_id bigint NOT NULL,
    seasons integer[] NOT NULL,
    sampled_fixtures integer NOT NULL,
    xg_fixtures integer NOT NULL,
    complete_xg_fixtures integer NOT NULL,
    sample_coverage_pct numeric NOT NULL,
    provider_supported boolean NOT NULL,
    sample_fixture_ids bigint[] NOT NULL,
    probed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: data_movements data_movement_id; Type: DEFAULT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.data_movements ALTER COLUMN data_movement_id SET DEFAULT nextval('data_detector.data_movements_data_movement_id_seq'::regclass);


--
-- Name: data_quality_events data_quality_event_id; Type: DEFAULT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.data_quality_events ALTER COLUMN data_quality_event_id SET DEFAULT nextval('data_detector.data_quality_events_data_quality_event_id_seq'::regclass);


--
-- Name: pipeline_runs pipeline_run_id; Type: DEFAULT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.pipeline_runs ALTER COLUMN pipeline_run_id SET DEFAULT nextval('data_detector.pipeline_runs_pipeline_run_id_seq'::regclass);


--
-- Name: table_snapshots table_snapshot_id; Type: DEFAULT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.table_snapshots ALTER COLUMN table_snapshot_id SET DEFAULT nextval('data_detector.table_snapshots_table_snapshot_id_seq'::regclass);


--
-- Name: dim_countries country_id; Type: DEFAULT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_countries ALTER COLUMN country_id SET DEFAULT nextval('dim.dim_countries_country_id_seq'::regclass);


--
-- Name: dim_league_seasons league_season_id; Type: DEFAULT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_league_seasons ALTER COLUMN league_season_id SET DEFAULT nextval('dim.dim_league_seasons_league_season_id_seq'::regclass);


--
-- Name: dim_leagues league_id; Type: DEFAULT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_leagues ALTER COLUMN league_id SET DEFAULT nextval('dim.dim_leagues_league_id_seq'::regclass);


--
-- Name: dim_teams team_id; Type: DEFAULT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_teams ALTER COLUMN team_id SET DEFAULT nextval('dim.dim_teams_team_id_seq'::regclass);


--
-- Name: dim_venue venue_id; Type: DEFAULT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_venue ALTER COLUMN venue_id SET DEFAULT nextval('dim.dim_venue_venue_id_seq'::regclass);


--
-- Name: entries id; Type: DEFAULT; Schema: forecast; Owner: -
--

ALTER TABLE ONLY forecast.entries ALTER COLUMN id SET DEFAULT nextval('forecast.entries_id_seq'::regclass);


--
-- Name: settlements id; Type: DEFAULT; Schema: forecast; Owner: -
--

ALTER TABLE ONLY forecast.settlements ALTER COLUMN id SET DEFAULT nextval('forecast.settlements_id_seq'::regclass);


--
-- Name: snapshots id; Type: DEFAULT; Schema: forecast; Owner: -
--

ALTER TABLE ONLY forecast.snapshots ALTER COLUMN id SET DEFAULT nextval('forecast.snapshots_id_seq'::regclass);


--
-- Name: card_versions id; Type: DEFAULT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.card_versions ALTER COLUMN id SET DEFAULT nextval('raw.card_versions_id_seq'::regclass);


--
-- Name: model_picks id; Type: DEFAULT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.model_picks ALTER COLUMN id SET DEFAULT nextval('raw.model_picks_id_seq'::regclass);


--
-- Name: model_result_versions id; Type: DEFAULT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.model_result_versions ALTER COLUMN id SET DEFAULT nextval('raw.model_result_versions_id_seq'::regclass);


--
-- Name: raw_fixtures fixture_id; Type: DEFAULT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_fixtures ALTER COLUMN fixture_id SET DEFAULT nextval('raw.raw_fixtures_fixture_id_seq'::regclass);


--
-- Name: daily_flow_checks daily_flow_checks_pkey; Type: CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.daily_flow_checks
    ADD CONSTRAINT daily_flow_checks_pkey PRIMARY KEY (check_date, dataset_id);


--
-- Name: data_movements data_movements_pkey; Type: CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.data_movements
    ADD CONSTRAINT data_movements_pkey PRIMARY KEY (data_movement_id);


--
-- Name: data_quality_events data_quality_events_pkey; Type: CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.data_quality_events
    ADD CONSTRAINT data_quality_events_pkey PRIMARY KEY (data_quality_event_id);


--
-- Name: dataset_dependencies dataset_dependencies_pkey; Type: CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.dataset_dependencies
    ADD CONSTRAINT dataset_dependencies_pkey PRIMARY KEY (dataset_id, upstream_id);


--
-- Name: datasets datasets_pkey; Type: CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.datasets
    ADD CONSTRAINT datasets_pkey PRIMARY KEY (dataset_id);


--
-- Name: pipeline_runs pipeline_runs_dag_id_task_id_run_id_key; Type: CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.pipeline_runs
    ADD CONSTRAINT pipeline_runs_dag_id_task_id_run_id_key UNIQUE (dag_id, task_id, run_id);


--
-- Name: pipeline_runs pipeline_runs_pkey; Type: CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.pipeline_runs
    ADD CONSTRAINT pipeline_runs_pkey PRIMARY KEY (pipeline_run_id);


--
-- Name: table_snapshots table_snapshots_pkey; Type: CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.table_snapshots
    ADD CONSTRAINT table_snapshots_pkey PRIMARY KEY (table_snapshot_id);


--
-- Name: dim_countries dim_countries_country_name_key; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_countries
    ADD CONSTRAINT dim_countries_country_name_key UNIQUE (country_name);


--
-- Name: dim_countries dim_countries_pkey; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_countries
    ADD CONSTRAINT dim_countries_pkey PRIMARY KEY (country_id);


--
-- Name: dim_league_seasons dim_league_seasons_league_id_season_key; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_league_seasons
    ADD CONSTRAINT dim_league_seasons_league_id_season_key UNIQUE (league_id, season);


--
-- Name: dim_league_seasons dim_league_seasons_pkey; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_league_seasons
    ADD CONSTRAINT dim_league_seasons_pkey PRIMARY KEY (league_season_id);


--
-- Name: dim_leagues dim_leagues_api_league_id_key; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_leagues
    ADD CONSTRAINT dim_leagues_api_league_id_key UNIQUE (api_league_id);


--
-- Name: dim_leagues dim_leagues_pkey; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_leagues
    ADD CONSTRAINT dim_leagues_pkey PRIMARY KEY (league_id);


--
-- Name: dim_teams dim_teams_api_team_id_key; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_teams
    ADD CONSTRAINT dim_teams_api_team_id_key UNIQUE (api_team_id);


--
-- Name: dim_teams dim_teams_pkey; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_teams
    ADD CONSTRAINT dim_teams_pkey PRIMARY KEY (team_id);


--
-- Name: dim_venue dim_venue_api_venue_id_key; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_venue
    ADD CONSTRAINT dim_venue_api_venue_id_key UNIQUE (api_venue_id);


--
-- Name: dim_venue dim_venue_pkey; Type: CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_venue
    ADD CONSTRAINT dim_venue_pkey PRIMARY KEY (venue_id);


--
-- Name: elo_rating elo_rating_pkey; Type: CONSTRAINT; Schema: elo; Owner: -
--

ALTER TABLE ONLY elo.elo_rating
    ADD CONSTRAINT elo_rating_pkey PRIMARY KEY (team_id, league_season_id, fixture_id, snapshot_type);


--
-- Name: team_elo_history team_elo_history_pkey; Type: CONSTRAINT; Schema: elo; Owner: -
--

ALTER TABLE ONLY elo.team_elo_history
    ADD CONSTRAINT team_elo_history_pkey PRIMARY KEY (team_id, fixture_id);


--
-- Name: entries entries_pkey; Type: CONSTRAINT; Schema: forecast; Owner: -
--

ALTER TABLE ONLY forecast.entries
    ADD CONSTRAINT entries_pkey PRIMARY KEY (id);


--
-- Name: entries entries_snapshot_id_fixture_id_event_key_key; Type: CONSTRAINT; Schema: forecast; Owner: -
--

ALTER TABLE ONLY forecast.entries
    ADD CONSTRAINT entries_snapshot_id_fixture_id_event_key_key UNIQUE (snapshot_id, fixture_id, event_key);


--
-- Name: settlements settlements_pkey; Type: CONSTRAINT; Schema: forecast; Owner: -
--

ALTER TABLE ONLY forecast.settlements
    ADD CONSTRAINT settlements_pkey PRIMARY KEY (id);


--
-- Name: snapshots snapshots_content_hash_key; Type: CONSTRAINT; Schema: forecast; Owner: -
--

ALTER TABLE ONLY forecast.snapshots
    ADD CONSTRAINT snapshots_content_hash_key UNIQUE (content_hash);


--
-- Name: snapshots snapshots_pkey; Type: CONSTRAINT; Schema: forecast; Owner: -
--

ALTER TABLE ONLY forecast.snapshots
    ADD CONSTRAINT snapshots_pkey PRIMARY KEY (id);


--
-- Name: batches batches_pkey; Type: CONSTRAINT; Schema: forecast_archive; Owner: -
--

ALTER TABLE ONLY forecast_archive.batches
    ADD CONSTRAINT batches_pkey PRIMARY KEY (bucket, source_table, last_id);


--
-- Name: checkpoints checkpoints_pkey; Type: CONSTRAINT; Schema: forecast_archive; Owner: -
--

ALTER TABLE ONLY forecast_archive.checkpoints
    ADD CONSTRAINT checkpoints_pkey PRIMARY KEY (bucket, source_table);


--
-- Name: big_dog_watch big_dog_watch_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.big_dog_watch
    ADD CONSTRAINT big_dog_watch_pkey PRIMARY KEY (fixture_id);


--
-- Name: card_versions card_versions_context_hash_version_key; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.card_versions
    ADD CONSTRAINT card_versions_context_hash_version_key UNIQUE (context_hash, version);


--
-- Name: card_versions card_versions_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.card_versions
    ADD CONSTRAINT card_versions_pkey PRIMARY KEY (id);


--
-- Name: consensus_pool consensus_pool_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.consensus_pool
    ADD CONSTRAINT consensus_pool_pkey PRIMARY KEY (run_ds, fixture_id, unified_market);


--
-- Name: engine_runs engine_runs_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.engine_runs
    ADD CONSTRAINT engine_runs_pkey PRIMARY KEY (run_ds);


--
-- Name: fixture_xg_attempts fixture_xg_attempts_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.fixture_xg_attempts
    ADD CONSTRAINT fixture_xg_attempts_pkey PRIMARY KEY (fixture_id);


--
-- Name: fixture_xg fixture_xg_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.fixture_xg
    ADD CONSTRAINT fixture_xg_pkey PRIMARY KEY (fixture_id);


--
-- Name: match_analysis match_analysis_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.match_analysis
    ADD CONSTRAINT match_analysis_pkey PRIMARY KEY (run_ds, fixture_id);


--
-- Name: model_analysis model_analysis_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.model_analysis
    ADD CONSTRAINT model_analysis_pkey PRIMARY KEY (run_ds, model, fixture_id);


--
-- Name: model_picks model_picks_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.model_picks
    ADD CONSTRAINT model_picks_pkey PRIMARY KEY (id);


--
-- Name: model_processing_state model_processing_state_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.model_processing_state
    ADD CONSTRAINT model_processing_state_pkey PRIMARY KEY (scope, entity_key);


--
-- Name: model_result_versions model_result_versions_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.model_result_versions
    ADD CONSTRAINT model_result_versions_pkey PRIMARY KEY (id);


--
-- Name: model_runs model_runs_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.model_runs
    ADD CONSTRAINT model_runs_pkey PRIMARY KEY (run_ds, model);


--
-- Name: raw_fixtures raw_fixtures_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_fixtures
    ADD CONSTRAINT raw_fixtures_pkey PRIMARY KEY (fixture_id);


--
-- Name: raw_league_standings_status raw_league_standings_status_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.raw_league_standings_status
    ADD CONSTRAINT raw_league_standings_status_pkey PRIMARY KEY (league_season_id);


--
-- Name: sportybet_odds sportybet_odds_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.sportybet_odds
    ADD CONSTRAINT sportybet_odds_pkey PRIMARY KEY (fixture_id, market, line, selection);


--
-- Name: xg_league_probes xg_league_probes_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.xg_league_probes
    ADD CONSTRAINT xg_league_probes_pkey PRIMARY KEY (api_league_id);


--
-- Name: data_detector_data_movements_status_idx; Type: INDEX; Schema: data_detector; Owner: -
--

CREATE INDEX data_detector_data_movements_status_idx ON data_detector.data_movements USING btree (status, detected_at DESC);


--
-- Name: data_detector_data_movements_target_idx; Type: INDEX; Schema: data_detector; Owner: -
--

CREATE INDEX data_detector_data_movements_target_idx ON data_detector.data_movements USING btree (target_schema, target_table, detected_at DESC);


--
-- Name: data_detector_pipeline_runs_dag_started_idx; Type: INDEX; Schema: data_detector; Owner: -
--

CREATE INDEX data_detector_pipeline_runs_dag_started_idx ON data_detector.pipeline_runs USING btree (dag_id, started_at DESC);


--
-- Name: data_detector_pipeline_runs_status_idx; Type: INDEX; Schema: data_detector; Owner: -
--

CREATE INDEX data_detector_pipeline_runs_status_idx ON data_detector.pipeline_runs USING btree (status);


--
-- Name: data_detector_quality_events_severity_idx; Type: INDEX; Schema: data_detector; Owner: -
--

CREATE INDEX data_detector_quality_events_severity_idx ON data_detector.data_quality_events USING btree (severity, detected_at DESC);


--
-- Name: data_detector_quality_events_table_idx; Type: INDEX; Schema: data_detector; Owner: -
--

CREATE INDEX data_detector_quality_events_table_idx ON data_detector.data_quality_events USING btree (table_schema, table_name, detected_at DESC);


--
-- Name: data_detector_table_snapshots_table_idx; Type: INDEX; Schema: data_detector; Owner: -
--

CREATE INDEX data_detector_table_snapshots_table_idx ON data_detector.table_snapshots USING btree (table_schema, table_name, snapshot_at DESC);


--
-- Name: dim_countries_country_name_uidx; Type: INDEX; Schema: dim; Owner: -
--

CREATE UNIQUE INDEX dim_countries_country_name_uidx ON dim.dim_countries USING btree (country_name);


--
-- Name: dim_league_seasons_bootstrap_done_idx; Type: INDEX; Schema: dim; Owner: -
--

CREATE INDEX dim_league_seasons_bootstrap_done_idx ON dim.dim_league_seasons USING btree (fixtures_bootstrap_done);


--
-- Name: dim_league_seasons_league_id_idx; Type: INDEX; Schema: dim; Owner: -
--

CREATE INDEX dim_league_seasons_league_id_idx ON dim.dim_league_seasons USING btree (league_id);


--
-- Name: dim_league_seasons_league_id_season_uidx; Type: INDEX; Schema: dim; Owner: -
--

CREATE UNIQUE INDEX dim_league_seasons_league_id_season_uidx ON dim.dim_league_seasons USING btree (league_id, season);


--
-- Name: dim_leagues_api_league_id_uidx; Type: INDEX; Schema: dim; Owner: -
--

CREATE UNIQUE INDEX dim_leagues_api_league_id_uidx ON dim.dim_leagues USING btree (api_league_id);


--
-- Name: dim_leagues_country_id_idx; Type: INDEX; Schema: dim; Owner: -
--

CREATE INDEX dim_leagues_country_id_idx ON dim.dim_leagues USING btree (country_id);


--
-- Name: dim_teams_api_team_id_uidx; Type: INDEX; Schema: dim; Owner: -
--

CREATE UNIQUE INDEX dim_teams_api_team_id_uidx ON dim.dim_teams USING btree (api_team_id);


--
-- Name: dim_teams_country_id_idx; Type: INDEX; Schema: dim; Owner: -
--

CREATE INDEX dim_teams_country_id_idx ON dim.dim_teams USING btree (country_id);


--
-- Name: dim_teams_venue_id_idx; Type: INDEX; Schema: dim; Owner: -
--

CREATE INDEX dim_teams_venue_id_idx ON dim.dim_teams USING btree (venue_id);


--
-- Name: dim_venue_api_venue_id_uidx; Type: INDEX; Schema: dim; Owner: -
--

CREATE UNIQUE INDEX dim_venue_api_venue_id_uidx ON dim.dim_venue USING btree (api_venue_id);


--
-- Name: dim_venue_country_id_idx; Type: INDEX; Schema: dim; Owner: -
--

CREATE INDEX dim_venue_country_id_idx ON dim.dim_venue USING btree (country_id);


--
-- Name: elo_rating_league_fixture_idx; Type: INDEX; Schema: elo; Owner: -
--

CREATE INDEX elo_rating_league_fixture_idx ON elo.elo_rating USING btree (league_season_id, fixture_id);


--
-- Name: elo_rating_team_fixture_idx; Type: INDEX; Schema: elo; Owner: -
--

CREATE INDEX elo_rating_team_fixture_idx ON elo.elo_rating USING btree (team_id, fixture_id);


--
-- Name: team_elo_history_team_kickoff_idx; Type: INDEX; Schema: elo; Owner: -
--

CREATE INDEX team_elo_history_team_kickoff_idx ON elo.team_elo_history USING btree (team_id, kickoff_utc DESC);


--
-- Name: forecast_entries_fixture; Type: INDEX; Schema: forecast; Owner: -
--

CREATE INDEX forecast_entries_fixture ON forecast.entries USING btree (fixture_id, event_key);


--
-- Name: forecast_settlements_entry; Type: INDEX; Schema: forecast; Owner: -
--

CREATE INDEX forecast_settlements_entry ON forecast.settlements USING btree (entry_id, id DESC);


--
-- Name: card_versions_fixture_context; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX card_versions_fixture_context ON raw.card_versions USING btree (fixture_id, context_hash, version DESC);


--
-- Name: model_analysis_run_idx; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX model_analysis_run_idx ON raw.model_analysis USING btree (run_ds, model);


--
-- Name: model_picks_run_idx; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX model_picks_run_idx ON raw.model_picks USING btree (run_ds, model);


--
-- Name: raw_league_standings_season_group_team_uidx; Type: INDEX; Schema: raw; Owner: -
--

CREATE UNIQUE INDEX raw_league_standings_season_group_team_uidx ON raw.raw_league_standings USING btree (league_season_id, group_name, api_team_id);


--
-- Name: raw_raw_fixtures_api_fixture_id_uidx; Type: INDEX; Schema: raw; Owner: -
--

CREATE UNIQUE INDEX raw_raw_fixtures_api_fixture_id_uidx ON raw.raw_fixtures USING btree (api_fixture_id);


--
-- Name: raw_raw_fixtures_api_league_id_idx; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX raw_raw_fixtures_api_league_id_idx ON raw.raw_fixtures USING btree (api_league_id);


--
-- Name: raw_raw_fixtures_fixture_id_idx; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX raw_raw_fixtures_fixture_id_idx ON raw.raw_fixtures USING btree (fixture_id);


--
-- Name: raw_raw_fixtures_kickoff_utc_idx; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX raw_raw_fixtures_kickoff_utc_idx ON raw.raw_fixtures USING btree (kickoff_utc);


--
-- Name: raw_raw_league_standings_league_idx; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX raw_raw_league_standings_league_idx ON raw.raw_league_standings USING btree (league_season_id);


--
-- Name: raw_raw_league_standings_rank_idx; Type: INDEX; Schema: raw; Owner: -
--

CREATE INDEX raw_raw_league_standings_rank_idx ON raw.raw_league_standings USING btree (league_season_id, rank);


--
-- Name: entries forecast_no_mutation; Type: TRIGGER; Schema: forecast; Owner: -
--

CREATE TRIGGER forecast_no_mutation BEFORE DELETE OR UPDATE OR TRUNCATE ON forecast.entries FOR EACH STATEMENT EXECUTE FUNCTION forecast.reject_mutation();


--
-- Name: settlements forecast_no_mutation; Type: TRIGGER; Schema: forecast; Owner: -
--

CREATE TRIGGER forecast_no_mutation BEFORE DELETE OR UPDATE OR TRUNCATE ON forecast.settlements FOR EACH STATEMENT EXECUTE FUNCTION forecast.reject_mutation();


--
-- Name: snapshots forecast_no_mutation; Type: TRIGGER; Schema: forecast; Owner: -
--

CREATE TRIGGER forecast_no_mutation BEFORE DELETE OR UPDATE OR TRUNCATE ON forecast.snapshots FOR EACH STATEMENT EXECUTE FUNCTION forecast.reject_mutation();


--
-- Name: data_movements data_movements_pipeline_run_id_fkey; Type: FK CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.data_movements
    ADD CONSTRAINT data_movements_pipeline_run_id_fkey FOREIGN KEY (pipeline_run_id) REFERENCES data_detector.pipeline_runs(pipeline_run_id);


--
-- Name: data_quality_events data_quality_events_pipeline_run_id_fkey; Type: FK CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.data_quality_events
    ADD CONSTRAINT data_quality_events_pipeline_run_id_fkey FOREIGN KEY (pipeline_run_id) REFERENCES data_detector.pipeline_runs(pipeline_run_id);


--
-- Name: dataset_dependencies dataset_dependencies_dataset_id_fkey; Type: FK CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.dataset_dependencies
    ADD CONSTRAINT dataset_dependencies_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES data_detector.datasets(dataset_id) ON DELETE CASCADE;


--
-- Name: dataset_dependencies dataset_dependencies_upstream_id_fkey; Type: FK CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.dataset_dependencies
    ADD CONSTRAINT dataset_dependencies_upstream_id_fkey FOREIGN KEY (upstream_id) REFERENCES data_detector.datasets(dataset_id) ON DELETE CASCADE;


--
-- Name: table_snapshots table_snapshots_pipeline_run_id_fkey; Type: FK CONSTRAINT; Schema: data_detector; Owner: -
--

ALTER TABLE ONLY data_detector.table_snapshots
    ADD CONSTRAINT table_snapshots_pipeline_run_id_fkey FOREIGN KEY (pipeline_run_id) REFERENCES data_detector.pipeline_runs(pipeline_run_id);


--
-- Name: dim_league_seasons dim_league_seasons_league_id_fkey; Type: FK CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_league_seasons
    ADD CONSTRAINT dim_league_seasons_league_id_fkey FOREIGN KEY (league_id) REFERENCES dim.dim_leagues(league_id);


--
-- Name: dim_leagues dim_leagues_country_id_fkey; Type: FK CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_leagues
    ADD CONSTRAINT dim_leagues_country_id_fkey FOREIGN KEY (country_id) REFERENCES dim.dim_countries(country_id);


--
-- Name: dim_teams dim_teams_country_id_fkey; Type: FK CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_teams
    ADD CONSTRAINT dim_teams_country_id_fkey FOREIGN KEY (country_id) REFERENCES dim.dim_countries(country_id);


--
-- Name: dim_teams dim_teams_venue_id_fkey; Type: FK CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_teams
    ADD CONSTRAINT dim_teams_venue_id_fkey FOREIGN KEY (venue_id) REFERENCES dim.dim_venue(venue_id);


--
-- Name: dim_venue dim_venue_country_id_fkey; Type: FK CONSTRAINT; Schema: dim; Owner: -
--

ALTER TABLE ONLY dim.dim_venue
    ADD CONSTRAINT dim_venue_country_id_fkey FOREIGN KEY (country_id) REFERENCES dim.dim_countries(country_id);


--
-- Name: entries entries_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: forecast; Owner: -
--

ALTER TABLE ONLY forecast.entries
    ADD CONSTRAINT entries_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES forecast.snapshots(id);


--
-- Name: settlements settlements_entry_id_fkey; Type: FK CONSTRAINT; Schema: forecast; Owner: -
--

ALTER TABLE ONLY forecast.settlements
    ADD CONSTRAINT settlements_entry_id_fkey FOREIGN KEY (entry_id) REFERENCES forecast.entries(id);


--
-- Name: model_processing_state model_processing_state_result_id_fkey; Type: FK CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.model_processing_state
    ADD CONSTRAINT model_processing_state_result_id_fkey FOREIGN KEY (result_id) REFERENCES raw.model_result_versions(id);


--
-- PostgreSQL database dump complete
--

