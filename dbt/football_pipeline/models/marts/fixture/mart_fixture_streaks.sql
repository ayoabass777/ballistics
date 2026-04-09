{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Mart: mart_fixture_streaks
-- Eligible streaks aligned to the upcoming fixture context
--------------------------------------------------------------------------------

with eligible as (
    select
        *
    from {{ ref('mart_team_streaks') }}
    where is_streak_eligible
      and fixture_id is not null
),

fixture_info as (
    select
        fixture_id,
        fixture_slug,
        team_pair_key,
        home_team_id,
        away_team_id
    from {{ ref('mart_fixture_info') }}
)

select
    e.fixture_id,
    e.kickoff_utc,
    e.team_id,
    e.team_name,
    e.opponent_id,
    e.opponent_name,
    e.league_season_id,
    e.league_name,
    e.event_name,
    e.streak_type,
    e.streak_length,
    e.polarity,
    e.scope,
    r.fixture_slug,
    r.team_pair_key,
    r.home_team_id,
    r.away_team_id,
    json_build_object(
        'event_name', e.event_name,
        'count', e.streak_length,
        'total', null,
        'scope', case
            when e.scope = 'all' then 'overall'
            else e.scope
        end,
        'polarity', e.polarity
    ) as fixture_streak_json
from eligible e
left join fixture_info r
  on r.fixture_id = e.fixture_id
