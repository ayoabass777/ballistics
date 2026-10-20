{{ config(materialized='table') }}

-- Fixture grain, every status (NS, FT, postponed, ...). The engine's only
-- fixture source: it must not read raw.raw_fixtures.
--
-- IDs are warehouse ids (team_id, league_id, league_season_id). The one
-- exception is api_league_id, kept while engine calibration maps, league
-- clustering and the Big dog state are keyed on it. Remove it once those
-- move to league_id.
--
-- LEFT joins on purpose: a fixture whose league season or team is not yet in
-- the dimensions still appears (with NULL ids) instead of silently vanishing.
-- Team names come from stg_dim_teams, the one source every model uses, so the
-- same team is spelled the same everywhere (the per-fixture raw names drop
-- accents and prefixes: "FC Heidenheim" for "1. FC Heidenheim"). The raw name
-- is the fallback while a team is not yet in the dimension.

with teams as (
    select api_team_id, team_id, team_name, logo_file_id
    from {{ ref('stg_dim_teams') }}
),

-- Format of each competition phase (round without matchday number): the most
-- games any team plays in it within the league season. 3+ means round-robin
-- (regular season, split rounds, play-off groups); 1-2 means single games or
-- two-legged ties.
phase_games as (
    select api_league_id, season, phase, max(n) as max_games_per_team
    from (
        select api_league_id, season, {{ fixture_phase('round') }} as phase,
               team, count(*) as n
        from {{ ref('stg_raw_fixtures') }}
        cross join lateral (values (home_api_team_id), (away_api_team_id)) as t(team)
        where round is not null
        group by 1, 2, 3, 4
    ) per_team
    group by 1, 2, 3
),

fixtures as (
    select rf.*,
           {{ fixture_phase('rf.round') }} as phase,
           case
               when rf.round is null then null
               when {{ fixture_knockout_name('rf.round') }} then 'knockout'
               when pg.max_games_per_team >= 3 then 'league'
               else 'knockout'
           end as stage
    from {{ ref('stg_raw_fixtures') }} as rf
    left join phase_games as pg
      on pg.api_league_id = rf.api_league_id
     and pg.season = rf.season
     and pg.phase = {{ fixture_phase('rf.round') }}
)

select
    rf.fixture_id,
    lc.league_season_id,
    lc.league_id,
    rf.api_league_id,
    lc.league_name,
    lc.country_name,
    lc.tier                         as league_tier,
    rf.season,
    lc.is_current,
    rf.kickoff_utc,
    rf.fixture_status,
    rf.is_played,
    ht.team_id                      as home_team_id,
    at.team_id                      as away_team_id,
    -- Crest image ids (S3 crests/teams/<id>); the API turns them into URLs.
    ht.logo_file_id                 as home_logo_file_id,
    at.logo_file_id                 as away_logo_file_id,
    coalesce(ht.team_name, rf.home_team_name) as home_team_name,
    coalesce(at.team_name, rf.away_team_name) as away_team_name,
    rf.home_team_halftime_goal,
    rf.away_team_halftime_goal,
    rf.home_team_fulltime_goal,
    rf.away_team_fulltime_goal,
    rf.home_fulltime_result,
    rf.away_fulltime_result,
    rf.round,
    rf.phase,
    rf.stage,
    rf.stage = 'knockout'           as is_knockout,
    -- Post-season: any knockout game, or a phase named as play-offs
    -- (e.g. Belgium's Conference League play-off group).
    (rf.stage = 'knockout' or rf.phase ~* 'play-?offs?') as is_playoff,
    rf.updated_at
from fixtures as rf
left join {{ ref('int_league_context') }} as lc
  on lc.api_league_id = rf.api_league_id
 and lc.season = rf.season
left join teams as ht on ht.api_team_id = rf.home_api_team_id
left join teams as at on at.api_team_id = rf.away_api_team_id
