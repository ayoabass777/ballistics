"""
Fetch league standings from the API and upsert into raw.raw_league_standings keyed by league_season_id + team_id.
"""
import argparse
import time
from typing import Any, Dict, List, Tuple

import psycopg2
from psycopg2.extras import execute_values

import etl.src.config as config
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger
from etl.src.extract_fixtures import _rate_limited_get

STANDINGS_ENDPOINT = "standings"
logger = get_logger(__name__)

RAW_TABLE = "raw.raw_league_standings"
UPSERT_CONFLICT = "(league_season_id, api_team_id)"

# Columns for upsert
COLUMNS = [
    "league_season_id",
    "api_league_id",
    "season",
    "api_team_id",
    "team_name",
    "rank",
    "points",
    "games_played",
    "wins",
    "draws",
    "losses",
    "goals_for",
    "goals_against",
    "goal_diff",
    "form",
]


def fetch_standings(api_league_id: int, season: int) -> List[Dict[str, Any]]:
    """
    Fetch standings for a league/season using the rate-limited GET utility.
    """
    params = {"league": api_league_id, "season": season}
    payload = _rate_limited_get(STANDINGS_ENDPOINT, params=params)
    return payload


def extract_standings(payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flatten API standings payload into list of dicts.
    """
    flattened: List[Dict[str, Any]] = []
    if not payload:
        return flattened

    # Response structure: list with one element containing "league"->"standings"-> list of tables
    try:
        tables = payload[0].get("league", {}).get("standings", [])
    except (IndexError, AttributeError):
        return flattened

    for table in tables:
        for row in table:
            team = row.get("team", {}) or {}
            stats = row.get("all", {}) or {}
            flattened.append(
                {
                    "api_team_id": team.get("id"),
                    "team_name": team.get("name"),
                    "rank": row.get("rank"),
                    "points": row.get("points"),
                    "games_played": stats.get("played"),
                    "wins": stats.get("win"),
                    "draws": stats.get("draw"),
                    "losses": stats.get("lose"),
                    "goals_for": stats.get("goals", {}).get("for"),
                    "goals_against": stats.get("goals", {}).get("against"),
                    "goal_diff": row.get("goalsDiff"),
                    "form": row.get("form"),
                }
            )
    return flattened


def get_league_seasons(cur: psycopg2.extensions.cursor) -> List[Tuple[int, int, int]]:
    """
    Fetch league_season_id, api_league_id, season.
    """
    cur.execute(
        """
        SELECT ls.league_season_id, l.api_league_id, ls.season
        FROM raw_stg.stg_dim_league_seasons ls
        JOIN raw_stg.stg_dim_leagues l ON ls.league_id = l.league_id
        WHERE ls.is_current
        """
    )
    return cur.fetchall()


def ensure_table(cur: psycopg2.extensions.cursor) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            league_season_id INT NOT NULL,
            api_league_id INT NOT NULL,
            season INT NOT NULL,
            api_team_id INT NOT NULL,
            team_name TEXT,
            rank INT,
            points INT,
            games_played INT,
            wins INT,
            draws INT,
            losses INT,
            goals_for INT,
            goals_against INT,
            goal_diff INT,
            form TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (league_season_id, api_team_id)
        )
        """
    )


def upsert_standings(rows: List[Dict[str, Any]]) -> int:
    """
    Upsert standings rows into RAW_TABLE.
    Deduplicates by (league_season_id, api_team_id) before insert — some leagues
    return multiple standings tables (e.g. playoff groups) with overlapping teams.
    """
    if not rows:
        return 0

    # Deduplicate: last occurrence wins (most current table)
    seen: Dict[Tuple[Any, Any], int] = {}
    deduped: List[Dict[str, Any]] = []
    for row in rows:
        key = (row.get("league_season_id"), row.get("api_team_id"))
        if key in seen:
            deduped[seen[key]] = row
        else:
            seen[key] = len(deduped)
            deduped.append(row)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        ensure_table(cur)
        values = [
            [
                row.get("league_season_id"),
                row.get("api_league_id"),
                row.get("season"),
                row.get("api_team_id"),
                row.get("team_name"),
                row.get("rank"),
                row.get("points"),
                row.get("games_played"),
                row.get("wins"),
                row.get("draws"),
                row.get("losses"),
                row.get("goals_for"),
                row.get("goals_against"),
                row.get("goal_diff"),
                row.get("form"),
            ]
            for row in deduped
        ]
        set_clause = ", ".join(
            f"{col}=EXCLUDED.{col}" for col in COLUMNS if col not in {"league_season_id", "api_team_id"}
        )
        insert_sql = f"""
            INSERT INTO {RAW_TABLE} ({', '.join(COLUMNS)})
            VALUES %s
            ON CONFLICT {UPSERT_CONFLICT}
            DO UPDATE SET {set_clause}, updated_at = NOW()
        """
        execute_values(cur, insert_sql, values, page_size=500)
        conn.commit()
        if len(deduped) < len(rows):
            logger.info("Deduplicated %d duplicate standings rows before upsert.", len(rows) - len(deduped))
        return len(deduped)
    except Exception as e:
        conn.rollback()
        logger.error("Failed to upsert standings: %s", e, exc_info=True)
        raise
    finally:
        cur.close()
        conn.close()


def update_standings() -> int:
    """
    Fetch standings for all current league seasons and upsert into DB.
    """
    start = time.time()
    total_rows = 0
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        league_rows = get_league_seasons(cur)
        if not league_rows:
            logger.info("No league seasons found for standings update.")
            return 0

        for league_season_id, api_league_id, season in league_rows:
            start_fetch = time.time()
            payload = fetch_standings(api_league_id, season)
            logger.info(
                "Fetched standings payload (len=%s) for league_season_id=%s in %.2fs",
                len(payload),
                league_season_id,
                time.time() - start_fetch,
            )
            extracted = extract_standings(payload)
            for row in extracted:
                row["league_season_id"] = league_season_id
                row["api_league_id"] = api_league_id
                row["season"] = season
            inserted = upsert_standings(extracted)
            total_rows += inserted
            logger.info(
                "Upserted %d standings rows for league_season_id=%s", inserted, league_season_id
            )
        logger.info("Standings update complete: %d rows in %.2fs", total_rows, time.time() - start)
        return total_rows
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update league standings from API.")
    args = parser.parse_args()
    try:
        update_standings()
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received; shutting down standings update.")
