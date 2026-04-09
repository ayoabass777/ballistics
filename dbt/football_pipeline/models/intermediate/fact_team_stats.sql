{{ config(materialized='table')}}

--------------------------------------------------------------------------------
-- Fact: fact_team_stats
-- Cumulative team performance metrics derived from fact_team_fixtures
--------------------------------------------------------------------------------

WITH base AS (
    SELECT
        f.fixture_id,
        f.kickoff_utc,
        f.season,
        lc.league_id,
        lc.country_name AS country,
        f.league_name AS league_name,
        f.league_season_id,
        f.team_id,
        f.team_name,
        f.opponent_id,
        f.opponent_name,
        f.is_home,
        f.fulltime_result AS result,
        f.goals_for AS goals_scored,
        f.goals_against AS goals_conceded,
        f.next_opponent_id,
        f.next_opponent_name,
        f.next_fixture_id,
        f.next_kickoff_utc
    FROM {{ ref('fact_team_fixtures') }} f
    LEFT JOIN {{ ref('int_league_context') }} lc
      ON f.league_season_id = lc.league_season_id
    WHERE f.is_played
),


games_played_stats AS (
    SELECT *,
        COUNT(*) OVER(
            PARTITION BY league_season_id, team_id
            ORDER BY kickoff_utc, fixture_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS games_played,
        SUM(CASE WHEN result ='win' THEN 1 ELSE 0 END) OVER(
            PARTITION BY league_season_id, team_id
            ORDER BY kickoff_utc, fixture_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS wins,
        SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) OVER(
            PARTITION BY league_season_id, team_id
            ORDER BY kickoff_utc, fixture_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS losses,
        SUM(CASE WHEN result='draw' THEN 1 ELSE 0 END) OVER(
            PARTITION BY league_season_id, team_id
            ORDER BY kickoff_utc, fixture_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS draws,
        --goals for is sum of goal scored 
        SUM(goals_scored) OVER(
            PARTITION BY league_season_id, team_id
            ORDER BY kickoff_utc, fixture_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS total_goals_for,
        --goals against is sum of goal conceded
        SUM(goals_conceded) OVER(
            PARTITION BY league_season_id, team_id
            ORDER BY kickoff_utc, fixture_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS total_goals_against
    FROM base
),

rolling_rates_and_averages AS (
    SELECT *,
        ROUND((wins:: FLOAT / games_played)::numeric, 2) AS rolling_win_rate,
        ROUND((losses:: FLOAT / games_played)::numeric, 2) AS rolling_loss_rate,
        ROUND((draws:: FLOAT / games_played)::numeric, 2) AS rolling_draw_rate,
        ROUND(AVG(goals_scored)
            OVER(PARTITION BY league_season_id, team_id 
                ORDER BY kickoff_utc, fixture_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::numeric, 2)AS rolling_AVG_goals_for,
        ROUND(AVG(goals_conceded) 
            OVER(PARTITION BY league_season_id, team_id 
                ORDER BY kickoff_utc, fixture_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::numeric, 2) AS rolling_AVG_goals_against
    FROM games_played_stats
),

rolling_pts AS (
    SELECT *,
        SUM(CASE WHEN result ='win' THEN 3
                WHEN result='draw' THEN 1
                ELSE 0
            END) OVER(
                PARTITION BY league_season_id, team_id
                ORDER BY kickoff_utc, fixture_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS rolling_points
    FROM rolling_rates_and_averages
),

final AS(
    SELECT *,
    ROW_NUMBER () OVER(PARTITION BY league_season_id, team_id
        ORDER BY kickoff_utc DESC, fixture_id DESC) AS rank
    FROM rolling_pts
),

form AS (
    SELECT
        league_season_id,
        team_id,
        ARRAY_AGG(result ORDER BY kickoff_utc DESC, fixture_id DESC) AS recent_form
    FROM (
        SELECT
            league_season_id,
            team_id,
            result,
            kickoff_utc,
            fixture_id,
            ROW_NUMBER() OVER (
                PARTITION BY league_season_id, team_id
                ORDER BY kickoff_utc DESC, fixture_id DESC
            ) AS rn
        FROM base
    ) ranked
    WHERE rn <= 5
    GROUP BY league_season_id, team_id
)

SELECT
    kickoff_utc,
    team_name,
    team_id,
    is_home,
    league_name,
    league_id,
    league_season_id,
    country,
    season,
    result,
    opponent_name,
    opponent_id,
    games_played,
    wins,
    losses,
    draws,
    goals_scored,
    goals_conceded,
    rolling_win_rate AS win_rate,
    rolling_draw_rate AS draw_rate,
    rolling_loss_rate AS loss_rate,
    rolling_AVG_goals_for AS avg_goals_for,
    rolling_AVG_goals_against AS avg_goals_against,
    next_opponent_id,
    next_opponent_name,
    next_fixture_id,
    next_kickoff_utc,
    rank,
    form.recent_form::text AS recent_form
FROM final
LEFT JOIN form USING (league_season_id, team_id)

    
