{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Intermediate: int_tier_sensitive_events
-- Which event × context combinations are worth opponent-tier conditioning
-- for each league_season_id, based on avg abs divergence threshold.
--
-- Logic:
--   1. Compute avg abs divergence across tiers per league_season × event × context
--   2. Flag as tier_sensitive when avg_abs_divergence >= threshold
--   3. Threshold has two components that are combined with GREATEST():
--
--      a) Dynamic sample-size floor — starts high (conservative) early in the
--         season when tier matchups are thin, relaxes as data accumulates:
--
--           floor = 0.03 + 0.03 * EXP(-min_tier_n / 60)
--
--         Where min_tier_n = fewest fixtures observed in any single tier
--         (the weakest tier is always the bottleneck).  Calibration:
--           min_tier_n = 15  → floor ≈ 0.053  (very conservative)
--           min_tier_n = 60  → floor ≈ 0.041  (close to old static 0.04)
--           min_tier_n = 150 → floor ≈ 0.032  (relaxed, data-rich late season)
--
--         This means the same divergence signal that was ignored in matchweek 5
--         (thin data, floor is high) will be correctly flagged in matchweek 30
--         (rich data, floor has fallen) — the threshold adapts to the season.
--
--      b) League median scale — keeps the threshold proportional to each
--         league's own divergence distribution so tight leagues (Championship)
--         aren't held to the same absolute bar as spread-out ones (Premier League):
--
--           LEAST(0.06, median_divergence * 1.2)
--
--   Final threshold = GREATEST(dynamic_floor, league_median_scale)
--   Capped at 0.06 to prevent the threshold from being set so high that nothing
--   ever gets flagged even in high-divergence leagues.
--
-- Consumed by the Python form pipeline via fetch_tier_sensitive_events_for_league().
--------------------------------------------------------------------------------

WITH avg_divergence AS (
    SELECT
        league_season_id,
        event_name,
        context_scope,
        AVG(abs_divergence)    AS avg_abs_divergence,
        COUNT(opponent_tier)   AS n_tiers,
        -- Minimum fixtures observed across tiers: the weakest tier is the
        -- bottleneck for how much we trust the divergence estimates.
        MIN(n_matches)         AS min_tier_n
    FROM {{ ref('int_tier_event_divergence') }}
    GROUP BY league_season_id, event_name, context_scope
),

league_median AS (
    -- Median avg_abs_divergence per league_season_id (used for adaptive threshold)
    SELECT
        league_season_id,
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY avg_abs_divergence
        ) AS median_divergence
    FROM avg_divergence
    GROUP BY league_season_id
),

with_threshold AS (
    SELECT
        ad.*,
        -- Dynamic sample-size floor: conservative when data is thin, relaxes
        -- as the season accumulates more tier-matchup observations.
        -- EXP decay anchored at min_tier_n=60 ≈ old static floor of 0.04.
        (0.03 + 0.03 * EXP(-ad.min_tier_n::float / 60.0)) AS dynamic_floor,
        -- League-median scale: keeps threshold proportional to this league's
        -- own divergence distribution (capped at 0.06).
        LEAST(0.06, lm.median_divergence * 1.2)            AS league_scale,
        GREATEST(
            0.03 + 0.03 * EXP(-ad.min_tier_n::float / 60.0),
            LEAST(0.06, lm.median_divergence * 1.2)
        )                                                   AS threshold
    FROM avg_divergence ad
    JOIN league_median lm USING (league_season_id)
)

SELECT
    league_season_id,
    event_name,
    context_scope,
    min_tier_n,
    ROUND(avg_abs_divergence::numeric, 6) AS avg_abs_divergence,
    ROUND(dynamic_floor::numeric,      6) AS dynamic_floor,
    ROUND(league_scale::numeric,       6) AS league_scale,
    ROUND(threshold::numeric,          6) AS threshold,
    (avg_abs_divergence >= threshold)     AS is_tier_sensitive
FROM with_threshold
ORDER BY league_season_id, event_name, context_scope
