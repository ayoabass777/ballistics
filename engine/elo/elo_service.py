"""
Elo service: walks played fixtures from raw.fact_head_to_head chronologically,
applies Elo updates, and writes per-team rating history to elo.team_elo_history.

Idempotent: re-runs only process fixtures not already in history.
Use --reset to wipe and rebuild from scratch.

Source contract (raw.fact_head_to_head):
    Two rows per fixture — one per team. We filter `is_home=true` to get a
    single canonical row per match containing both teams' identities and the
    final score.

Usage:
    python -m engine.elo.elo_service              # incremental — process new fixtures
    python -m engine.elo.elo_service --reset      # full rebuild from scratch
"""

import argparse
import time
from typing import Any, Dict, List, Tuple

import psycopg2  # noqa: F401  (kept for type-clarity; conn type matches project style)
from psycopg2.extras import execute_values

from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger
from engine.elo.elo_math import (
    INITIAL_RATING,
    K_FACTOR,
    HOME_ADVANTAGE,
    expected_score,
    actual_score,
    update_rating,
    adaptive_k,
)

logger = get_logger(__name__)

ELO_SCHEMA = "elo"
HISTORY_TABLE = f"{ELO_SCHEMA}.team_elo_history"
SOURCE_TABLE = "raw.fact_head_to_head"


# --------------------------------------------------------------------------- #
# Schema setup
# --------------------------------------------------------------------------- #

def ensure_schema_and_table(cur) -> None:
    """Create the elo schema, history table, and supporting index if absent."""
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {ELO_SCHEMA}")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
            team_id        INT         NOT NULL,
            fixture_id     INT         NOT NULL,
            kickoff_utc    TIMESTAMPTZ NOT NULL,
            rating_before  FLOAT       NOT NULL,
            rating_after   FLOAT       NOT NULL,
            expected_score FLOAT       NOT NULL,
            actual_score   FLOAT       NOT NULL,
            k_value        INT         NOT NULL,
            rating_before_flat FLOAT,
            rating_after_flat  FLOAT,
            season         TEXT,
            league_name    TEXT,
            computed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (team_id, fixture_id)
        )
        """
    )
    # Add flat-K columns if upgrading from older schema
    cur.execute(f"""
        DO $$ BEGIN
            ALTER TABLE {HISTORY_TABLE} ADD COLUMN rating_before_flat FLOAT;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$
    """)
    cur.execute(f"""
        DO $$ BEGIN
            ALTER TABLE {HISTORY_TABLE} ADD COLUMN rating_after_flat FLOAT;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$
    """)
    # Latest-rating-per-team lookups hit this hard. Worth the index.
    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS team_elo_history_team_kickoff_idx
        ON {HISTORY_TABLE} (team_id, kickoff_utc DESC)
        """
    )


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #

def fetch_unprocessed_fixtures(cur) -> List[Tuple]:
    """
    Pull played fixtures not yet in elo history, ordered chronologically.

    We only need the home-perspective row from fact_head_to_head — it contains
    both team_id (home) and opponent_id (away), so one row = one full match.
    """
    cur.execute(
        f"""
        SELECT
            f.fixture_id,
            f.kickoff_utc,
            f.team_id       AS home_team_id,
            f.opponent_id   AS away_team_id,
            f.goals_for     AS home_goals,
            f.goals_against AS away_goals,
            f.season,
            f.league_name
        FROM {SOURCE_TABLE} f
        WHERE f.is_played
          AND f.is_home
          AND NOT EXISTS (
              SELECT 1
              FROM {HISTORY_TABLE} h
              WHERE h.fixture_id = f.fixture_id
                AND h.team_id    = f.team_id
          )
        ORDER BY f.kickoff_utc, f.fixture_id
        """
    )
    return cur.fetchall()


def get_current_ratings(cur, team_ids: List[int]) -> Dict[int, Tuple[float, float]]:
    """
    For each team, fetch its most recent (adaptive, flat) ratings.
    Teams not yet seen are absent from the result — caller defaults them.
    """
    if not team_ids:
        return {}
    cur.execute(
        f"""
        SELECT DISTINCT ON (team_id)
            team_id, rating_after, COALESCE(rating_after_flat, rating_after)
        FROM {HISTORY_TABLE}
        WHERE team_id = ANY(%s)
        ORDER BY team_id, kickoff_utc DESC, fixture_id DESC
        """,
        (team_ids,),
    )
    return {row[0]: (row[1], row[2]) for row in cur.fetchall()}


# --------------------------------------------------------------------------- #
# League volatility
# --------------------------------------------------------------------------- #

def compute_league_upset_rates(cur) -> Dict[str, float]:
    """
    Compute upset rate per league from Elo history.
    An upset = the team with lower Elo before kickoff wins.
    Returns {league_name: upset_rate}.
    """
    cur.execute(
        f"""
        WITH match_elos AS (
            SELECT
                h.fixture_id,
                h.league_name,
                h.rating_before  AS home_elo,
                h.actual_score   AS home_result,
                a.rating_before  AS away_elo
            FROM {HISTORY_TABLE} h
            JOIN {HISTORY_TABLE} a
              ON a.fixture_id = h.fixture_id AND a.team_id != h.team_id
            JOIN {SOURCE_TABLE} f
              ON f.fixture_id = h.fixture_id AND f.team_id = h.team_id AND f.is_home
            WHERE h.league_name IS NOT NULL
        )
        SELECT
            league_name,
            COUNT(*) AS games,
            AVG(CASE
                WHEN home_elo > away_elo AND home_result = 0.0 THEN 1
                WHEN away_elo > home_elo AND home_result = 1.0 THEN 1
                ELSE 0
            END) AS upset_rate
        FROM match_elos
        WHERE home_elo != away_elo
        GROUP BY league_name
        HAVING COUNT(*) >= 50
        """
    )
    return {row[0]: float(row[2]) for row in cur.fetchall()}


# --------------------------------------------------------------------------- #
# Main job
# --------------------------------------------------------------------------- #

def process_fixtures(reset: bool = False) -> Dict[str, int]:
    """
    Main entry point. Walks fixtures chronologically, applies Elo updates.

    If reset=True, truncates the history table first (full rebuild).
    Otherwise, processes only fixtures not yet in history.

    Returns a small summary dict for logging / orchestration.
    """
    start = time.time()
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        ensure_schema_and_table(cur)

        # Compute upset rates BEFORE truncating so reset rebuilds can use them.
        league_upset = compute_league_upset_rates(cur)
        league_k: Dict[str, int] = {
            league: adaptive_k(rate) for league, rate in league_upset.items()
        }

        if reset:
            logger.warning("Reset requested — truncating %s", HISTORY_TABLE)
            cur.execute(f"TRUNCATE {HISTORY_TABLE}")

        fixtures = fetch_unprocessed_fixtures(cur)
        if not fixtures:
            logger.info("No unprocessed fixtures. Nothing to do.")
            conn.commit()
            return {"fixtures_processed": 0, "rows_written": 0}

        logger.info("Processing %d fixtures…", len(fixtures))
        if league_k:
            logger.info(
                "Adaptive K: %s",
                ", ".join(f"{l}={k}" for l, k in sorted(league_k.items())),
            )

        # Seed in-memory rating caches from history (for incremental runs).
        all_team_ids: set = set()
        for f in fixtures:
            all_team_ids.add(f[2])  # home_team_id
            all_team_ids.add(f[3])  # away_team_id
        cached = get_current_ratings(cur, list(all_team_ids))
        # Dual-track: adaptive ratings + flat K=30 ratings
        ratings: Dict[int, float] = {t: v[0] for t, v in cached.items()}
        ratings_flat: Dict[int, float] = {t: v[1] for t, v in cached.items()}

        rows_to_insert: List[Tuple[Any, ...]] = []
        for (
            fixture_id,
            kickoff_utc,
            home_id,
            away_id,
            home_g,
            away_g,
            season,
            league_name,
        ) in fixtures:
            r_home = ratings.get(home_id, INITIAL_RATING)
            r_away = ratings.get(away_id, INITIAL_RATING)
            rf_home = ratings_flat.get(home_id, INITIAL_RATING)
            rf_away = ratings_flat.get(away_id, INITIAL_RATING)

            # Adaptive-K track
            e_home = expected_score(r_home + HOME_ADVANTAGE, r_away)
            e_away = 1.0 - e_home
            s_home = actual_score(home_g, away_g)
            s_away = 1.0 - s_home
            k = league_k.get(league_name, K_FACTOR)
            r_home_new = update_rating(r_home, e_home, s_home, k=k)
            r_away_new = update_rating(r_away, e_away, s_away, k=k)

            # Flat-K track (K=30, same expected/actual scores)
            ef_home = expected_score(rf_home + HOME_ADVANTAGE, rf_away)
            ef_away = 1.0 - ef_home
            rf_home_new = update_rating(rf_home, ef_home, s_home, k=K_FACTOR)
            rf_away_new = update_rating(rf_away, ef_away, s_away, k=K_FACTOR)

            rows_to_insert.append(
                (home_id, fixture_id, kickoff_utc, r_home, r_home_new,
                 e_home, s_home, k, rf_home, rf_home_new, season, league_name)
            )
            rows_to_insert.append(
                (away_id, fixture_id, kickoff_utc, r_away, r_away_new,
                 e_away, s_away, k, rf_away, rf_away_new, season, league_name)
            )

            ratings[home_id] = r_home_new
            ratings[away_id] = r_away_new
            ratings_flat[home_id] = rf_home_new
            ratings_flat[away_id] = rf_away_new

        insert_sql = f"""
            INSERT INTO {HISTORY_TABLE} (
                team_id, fixture_id, kickoff_utc,
                rating_before, rating_after,
                expected_score, actual_score, k_value,
                rating_before_flat, rating_after_flat,
                season, league_name
            ) VALUES %s
            ON CONFLICT (team_id, fixture_id) DO NOTHING
        """
        execute_values(cur, insert_sql, rows_to_insert, page_size=1000)
        conn.commit()

        elapsed = time.time() - start
        logger.info(
            "Elo run complete: %d fixtures, %d rows written in %.2fs",
            len(fixtures), len(rows_to_insert), elapsed,
        )
        return {
            "fixtures_processed": len(fixtures),
            "rows_written": len(rows_to_insert),
        }

    except Exception as e:
        conn.rollback()
        logger.error("Elo processing failed: %s", e, exc_info=True)
        raise
    finally:
        cur.close()
        conn.close()


# --------------------------------------------------------------------------- #
# Read helpers (used by downstream services — e.g. matchup predictions)
# --------------------------------------------------------------------------- #

def get_latest_rating(cur, team_id: int) -> float:
    """Return the team's current Elo rating, or INITIAL_RATING if unseen."""
    cur.execute(
        f"""
        SELECT rating_after
        FROM {HISTORY_TABLE}
        WHERE team_id = %s
        ORDER BY kickoff_utc DESC, fixture_id DESC
        LIMIT 1
        """,
        (team_id,),
    )
    row = cur.fetchone()
    return row[0] if row else INITIAL_RATING


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute Elo ratings from played fixtures in raw.fact_head_to_head."
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Wipe elo.team_elo_history and recompute from scratch.",
    )
    args = parser.parse_args()
    try:
        result = process_fixtures(reset=args.reset)
        print(f"Done: {result}")
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
