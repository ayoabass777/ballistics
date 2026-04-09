{{ config (materialized='table')}}


--------------------------------------------------------------------------------
-- -- Intermediate: fact_team_streak_relevance_score
-- -- Calculate relevance scores for team streaks based on length, weight and multipliers
--------------------------------------------------------------------------------

WITH streaks AS (
    SELECT
        team_id,
        team_name,
        league_season_id,
        league_name,
        streak_id,
        event_name,
        streak_type,
        composition,
        polarity,
        threshold,
        context_scope,
        current_streak_length,
        streak_length,
        kickoff_utc,
        fixture_id,
        opponent_id,
        opponent_name,
        is_home,
        next_fixture_id,
        next_kickoff_utc,
        next_opponent_id,
        next_opponent_name,
        avg_goals_for_context,
        avg_goals_against_context,
        is_streak_eligible
    FROM {{ ref('fact_team_streak_suppresion') }}
    WHERE NOT is_suppressed AND is_active
),

category_weights AS (
    SELECT category, weight
    FROM {{ ref('streak_category_weights') }}
),

threshold_weights AS (
    SELECT threshold, multiplier
    FROM {{ ref('streak_goal_threshold_multipliers')}}
),

composition_weights AS (
    SELECT composition, multiplier
    FROM {{ ref('streak_composition_multipliers')}}
)

SELECT 
    s.*,

    cw.weight
    * log(2, s.current_streak_length + 1)
    * COALESCE(tw.multiplier, 1.0)
    * COALESCE(cpw.multiplier, 1.0) AS relevance_score

FROM streaks s
LEFT JOIN category_weights cw
    ON s.streak_type = cw.category
LEFT JOIN threshold_weights tw
    ON s.threshold = tw.threshold
LEFT JOIN composition_weights AS cpw 
    ON s.composition = cpw.composition
