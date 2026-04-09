{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Intermediate: int_opponent_tier_mu
-- Opponent-tier-adjusted league mean (μ) per event × context × opponent tier.
--
-- Opponent tier is bucketed into terciles (1=weak, 2=mid, 3=strong) within
-- each league_season_id, using the opponent's pre-match Elo rating.
--
-- Use this in place of the unconditional league mean for result and team-goal
-- events (win, loss, clean_sheet, score_2goals, concede_2, score_1goal).
-- For totals markets (Over/Under, BTTS) the unconditional mean is sufficient.
--
-- Minimum observations guard: rows with fewer than 15 fixtures in a tier are
-- suppressed entirely.  The Python caller (fetch_opponent_tier_mu) falls back
-- silently to the unconditional μ when a tier row is absent, so thin early-
-- season or small-league tiers never inject a noisy adjustment.
-- 15 is a practical floor: below it, a single freak result can shift the
-- tier rate by 6+ percentage points, which exceeds any real signal.
--------------------------------------------------------------------------------

WITH opponent_elo AS (
    SELECT
        team_id        AS opponent_id,
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
)

SELECT
    league_season_id,
    event_name,
    context_scope,
    opponent_tier,
    COUNT(*)                                  AS n_matches,
    SUM(event_flag)                           AS n_occurrences,
    ROUND(AVG(event_flag::float)::numeric, 6) AS mu_tier
FROM events_with_tier
GROUP BY
    league_season_id,
    event_name,
    context_scope,
    opponent_tier
-- Suppress tiers backed by fewer than 15 fixtures.  A single result in a thin
-- tier can shift the rate by 6+ pp, which swamps any real signal.  Python falls
-- back to the unconditional μ when a tier row is absent.
HAVING COUNT(*) >= 15
