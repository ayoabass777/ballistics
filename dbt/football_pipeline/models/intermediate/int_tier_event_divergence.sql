{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Intermediate: int_tier_event_divergence
-- Avg absolute divergence of event rate vs unconditional league mean,
-- per league_season_id × event_name × context_scope × opponent_tier.
--
-- Used by int_tier_sensitive_events to determine which events warrant
-- opponent-tier conditioning in the Bayesian form model.
--------------------------------------------------------------------------------

WITH opponent_elo AS (
    SELECT
        team_id       AS opponent_id,
        fixture_id,
        pre_elo_rating AS opponent_pre_elo
    FROM elo.elo_rating
    WHERE snapshot_type = 'match'
),

events_with_tier AS (
    SELECT
        fte.league_season_id,
        fte.event_name,
        fte.context_scope,
        fte.event_flag,
        NTILE(3) OVER (
            PARTITION BY fte.league_season_id
            ORDER BY oe.opponent_pre_elo
        ) AS opponent_tier
    FROM {{ ref('fact_team_events') }} fte
    JOIN opponent_elo oe
        ON oe.opponent_id = fte.opponent_id
       AND oe.fixture_id  = fte.fixture_id
    WHERE fte.is_played
),

tier_rates AS (
    SELECT
        league_season_id,
        event_name,
        context_scope,
        opponent_tier,
        COUNT(*)               AS n_matches,
        AVG(event_flag::float) AS event_rate
    FROM events_with_tier
    GROUP BY league_season_id, event_name, context_scope, opponent_tier
),

with_unconditional AS (
    SELECT
        *,
        AVG(event_rate) OVER (
            PARTITION BY league_season_id, event_name, context_scope
        ) AS unconditional_rate
    FROM tier_rates
)

SELECT
    league_season_id,
    event_name,
    context_scope,
    opponent_tier,
    n_matches,
    ROUND(event_rate::numeric, 6)                             AS event_rate,
    ROUND(unconditional_rate::numeric, 6)                     AS unconditional_rate,
    ROUND(ABS(event_rate - unconditional_rate)::numeric, 6)   AS abs_divergence
FROM with_unconditional
