{{ config(
    materialized='incremental',
    unique_key=['fixture_id', 'team_id'],
    on_schema_change='append_new_columns'
) }}

--------------------------------------------------------------------------------
-- Fact model: team_fixture_stats
-- One row per team×fixture with home/away split and results
-- Stores all team snapshot data needed for downstream analysis including next fixture lead(1)
--------------------------------------------------------------------------------




-- 2. Unpivot: explode home/away into a single team row
with base as (
    select
        df.fixture_id,
        df.kickoff_utc,
        df.league_season_id,
        df.league_name,
        df.season,
        team.team_id,
        team.team_name,
        team.is_home,
        team.goals_for,
        team.goals_against,
        team.ht_goals_for AS halftime_goals_for,
        team.ht_goals_against AS halftime_goals_against,
        team.fulltime_result,
        team.halftime_result,
        team.opponent_id,
        team.opponent_name,
        df.updated_at,
        df.is_played,
        df.is_current
    from {{ ref('int_fixtures') }} df
    cross join lateral (
        values
            (df.home_team_id, df.home_team_name,  true,  df.home_ftg, df.away_ftg, df.home_htg, df.away_htg, df.home_ftr, df.home_htr, df.away_team_id, df.away_team_name),
            (df.away_team_id,  df.away_team_name, false, df.away_ftg, df.home_ftg, df.away_htg, df.home_htg, df.away_ftr, df.away_htr, df.home_team_id, df.home_team_name)
    ) as team(
        team_id,
        team_name,
        is_home,
        goals_for,
        goals_against,
        ht_goals_for,
        ht_goals_against,
        fulltime_result,
        halftime_result,
        opponent_id,
        opponent_name
    )
    WHERE df.fixture_status IN ('FT', '1H', 'HT', '2H', 'LIVE', 'NS')
),


staged_with_upcoming_fx as (
    select *,
        lead(opponent_id) over(partition by team_id order by kickoff_utc) as next_opponent_id,
        lead(kickoff_utc) over(partition by team_id order by kickoff_utc) as next_kickoff_utc,
        lead(fixture_id) over(partition by team_id order by kickoff_utc) as next_fixture_id,
        lead(opponent_name) over(partition by team_id order by kickoff_utc) as next_opponent_name,
        lead(is_home) over(partition by team_id order by kickoff_utc) as next_is_home
    from base
)

select
    fixture_id,
    league_season_id,
    league_name,
    season,
    team_id,
    team_name,
    opponent_id,
    opponent_name,
    concat_ws('_', least(team_id, opponent_id), greatest(team_id, opponent_id)) as team_pair_key,
    is_home,
    kickoff_utc,
    goals_for,
    goals_against,
    halftime_goals_for,
    halftime_goals_against,
    fulltime_result,
    halftime_result,
    updated_at,
    is_played,
    is_current,
    next_opponent_id,
    next_opponent_name,
    next_fixture_id,
    next_kickoff_utc,
    next_is_home
from staged_with_upcoming_fx
