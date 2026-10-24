"""
Fetch league standings from the API and upsert into raw.raw_league_standings
keyed by league_season_id + group_name + api_team_id.

API-Football returns one table per group: a single table for most leagues,
parallel tables for conferences (MLS, USL), and extra tables for split
seasons (championship / relegation rounds). Each table is kept as its own
group; nothing is merged or overwritten.
"""
import argparse
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
from psycopg2.extras import Json, execute_values

import etl.src.config as config
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger
from etl.src.extract_fixtures import _rate_limited_get

STANDINGS_ENDPOINT = "standings"
logger = get_logger(__name__)

RAW_TABLE = "raw.raw_league_standings"
UPSERT_CONFLICT = "(league_season_id, group_name, api_team_id)"

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
    "home_record",
    "away_record",
    "group_name",
    "group_order",
    "description",
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
    except (IndexError, AttributeError, TypeError) as exc:
        return [{"_dlq_reason": f"invalid_standings_payload: {exc}", "_raw_payload": payload}]
    if not isinstance(tables, list):
        return [{"_dlq_reason": "invalid_standings_tables", "_raw_payload": payload}]
    tables = [table if isinstance(table, list) else [table] for table in tables]

    labels = [_table_label(table, i) for i, table in enumerate(tables)]
    repeated = {lab for lab in labels if labels.count(lab) > 1}

    for group_order, table in enumerate(tables):
        label = labels[group_order]
        if label in repeated:
            # The API sometimes gives parallel tables the same label (USL
            # Championship conferences both say "USL Championship").
            label = f"{label} - Table {group_order + 1}"
        for row in table:
            try:
                team = row.get("team", {}) or {}
                stats = row.get("all", {}) or {}
                home = row.get("home")
                away = row.get("away")
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
                        "home_record": home if isinstance(home, dict) else None,
                        "away_record": away if isinstance(away, dict) else None,
                        "group_name": label,
                        "group_order": group_order,
                        "description": row.get("description"),
                    }
                )
            except (AttributeError, TypeError, ValueError) as exc:
                flattened.append({"_dlq_reason": f"extraction_error: {exc}",
                                  "_raw_payload": row})
    return flattened


def drop_rows_without_team_id(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split rows into (usable, missing team id).

    The API occasionally sends a standings row with team.id null (Club León,
    Liga MX, 15 Oct 2026). api_team_id is part of the primary key, so one such
    row used to fail the whole league's upsert and, with it, the standings task
    and every stage downstream.
    """
    kept = [r for r in rows if r.get("api_team_id") is not None]
    missing = [r for r in rows if r.get("api_team_id") is None]
    return kept, missing


def _table_label(table: List[Dict[str, Any]], group_order: int) -> str:
    """Table label ("Eastern Conference", "Championship Round") from the
    /standings rows; tables without one are named by position so parallel
    tables still get distinct keys instead of overwriting each other."""
    for row in table:
        if isinstance(row, dict) and row.get("group"):
            return row["group"]
    return f"Table {group_order + 1}"


def get_league_seasons(
    cur: psycopg2.extensions.cursor, league_season_ids: Optional[List[int]] = None
) -> List[Tuple[int, int, int]]:
    """
    Fetch league_season_id, api_league_id, season: current seasons, or the
    given ids (backfill of past seasons).
    """
    sql = """
        SELECT ls.league_season_id, l.api_league_id, ls.season
        FROM raw_stg.stg_dim_league_seasons ls
        JOIN raw_stg.stg_dim_leagues l ON ls.league_id = l.league_id
    """
    if league_season_ids:
        cur.execute(sql + " WHERE ls.league_season_id = ANY(%s)", (list(league_season_ids),))
    else:
        cur.execute(sql + " WHERE ls.is_current")
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
            home_record JSONB,
            away_record JSONB,
            group_name TEXT NOT NULL DEFAULT '',
            group_order INT,
            description TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    migrate_groups(cur)


def migrate_groups(cur: psycopg2.extensions.cursor) -> None:
    """Idempotent: add group columns and key standings by group (2026-10-11).

    Rows written before this keep group_name '' and stop being refreshed;
    dbt's mart_league_standings shows only each league season's latest
    refresh, so they drop out of display on the next run.
    """
    cur.execute(f"""
        ALTER TABLE {RAW_TABLE} ADD COLUMN IF NOT EXISTS group_name TEXT NOT NULL DEFAULT '';
        ALTER TABLE {RAW_TABLE} ADD COLUMN IF NOT EXISTS group_order INT;
        ALTER TABLE {RAW_TABLE} ADD COLUMN IF NOT EXISTS description TEXT;
        ALTER TABLE {RAW_TABLE} ADD COLUMN IF NOT EXISTS home_record JSONB;
        ALTER TABLE {RAW_TABLE} ADD COLUMN IF NOT EXISTS away_record JSONB;
        ALTER TABLE {RAW_TABLE}
            DROP CONSTRAINT IF EXISTS raw_league_standings_league_season_id_api_team_id_key;
        CREATE UNIQUE INDEX IF NOT EXISTS raw_league_standings_season_group_team_uidx
            ON {RAW_TABLE} (league_season_id, group_name, api_team_id);
    """)


def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop exact repeats of (league_season, group, team); last one wins.

    A team in two groups (regular season + championship round) is two
    rows, not a duplicate.
    """
    seen: Dict[Tuple[Any, Any, Any], int] = {}
    deduped: List[Dict[str, Any]] = []
    for row in rows:
        key = (row.get("league_season_id"), row.get("group_name") or "",
               row.get("api_team_id"))
        if key in seen:
            deduped[seen[key]] = row
        else:
            seen[key] = len(deduped)
            deduped.append(row)
    return deduped


def prune_stale_rows(cur: psycopg2.extensions.cursor, rows: List[Dict[str, Any]]) -> int:
    """Delete a refreshed league season's rows that the new payload doesn't
    have: pre-groups rows (group_name ''), relabelled groups, teams that left
    a table. Only seasons present in `rows` are touched, so an empty API
    response never wipes a season."""
    by_season: Dict[Any, List[Tuple[str, Any]]] = {}
    for row in rows:
        by_season.setdefault(row.get("league_season_id"), []).append(
            (row.get("group_name") or "", row.get("api_team_id"))
        )
    deleted = 0
    for league_season_id, keys in by_season.items():
        cur.execute(
            f"""
            DELETE FROM {RAW_TABLE}
            WHERE league_season_id = %s
              AND (group_name, api_team_id) NOT IN %s
            """,
            (league_season_id, tuple(keys)),
        )
        deleted += cur.rowcount or 0
    return deleted


def upsert_standings(rows: List[Dict[str, Any]], prune: bool = True) -> int:
    """
    Upsert standings rows into RAW_TABLE, one row per (league season, group,
    team). One transaction per call, so a league season's refresh shares one
    updated_at.
    """
    if not rows:
        return 0

    deduped = dedupe_rows(rows)

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
                Json(row["home_record"]) if row.get("home_record") is not None else None,
                Json(row["away_record"]) if row.get("away_record") is not None else None,
                row.get("group_name") or "",
                row.get("group_order"),
                row.get("description"),
            ]
            for row in deduped
        ]
        set_clause = ", ".join(
            f"{col}=EXCLUDED.{col}" for col in COLUMNS
            if col not in {"league_season_id", "group_name", "api_team_id"}
        )
        insert_sql = f"""
            INSERT INTO {RAW_TABLE} ({', '.join(COLUMNS)})
            VALUES %s
            ON CONFLICT {UPSERT_CONFLICT}
            DO UPDATE SET {set_clause}, updated_at = NOW()
        """
        execute_values(cur, insert_sql, values, page_size=500)
        # Skip pruning when rows were dropped, or the dropped team's last good
        # row would be deleted as "no longer in the API response".
        pruned = prune_stale_rows(cur, deduped) if prune else 0
        conn.commit()
        if pruned:
            logger.info("Pruned %d standings rows no longer in the API response.", pruned)
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


def update_standings(
    return_summary: bool = False, league_season_ids: Optional[List[int]] = None,
    ds: Optional[str] = None,
) -> Union[int, Dict[str, Any]]:
    """
    Fetch standings for all current league seasons and upsert into DB.

    Rows that can't be loaded (no team id) go to the standings DLQ on S3 and
    flag the league (etl/src/standings_dlq.py, D147); the rest still load.
    """
    from datetime import date
    from etl.src.standings_dlq import refresh_league
    ds = ds or date.today().isoformat()
    start = time.time()
    total_rows = 0
    leagues_updated: List[Dict[str, Any]] = []
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        league_rows = get_league_seasons(cur, league_season_ids)
        if not league_rows:
            logger.info("No league seasons found for standings update.")
            if return_summary:
                return {
                    "total_rows": 0,
                    "league_count": 0,
                    "leagues_updated": [],
                }
            return 0

        unprocessed: List[Dict[str, Any]] = []
        for league_season_id, api_league_id, season in league_rows:
            start_fetch = time.time()
            payload = fetch_standings(api_league_id, season)
            logger.info(
                "Fetched standings payload (len=%s) for league_season_id=%s in %.2fs",
                len(payload),
                league_season_id,
                time.time() - start_fetch,
            )
            league = {"league_season_id": league_season_id,
                      "api_league_id": api_league_id, "season": season}
            inserted, parked = refresh_league(league, ds, source="nightly", payload=payload)
            if parked:
                unprocessed.append({**league, "teams": [str(r.get("team_name")) for r in parked]})
            total_rows += inserted
            leagues_updated.append(
                {
                    "league_season_id": league_season_id,
                    "api_league_id": api_league_id,
                    "season": season,
                    "row_count": inserted,
                    "unprocessed_rows": len(parked),
                }
            )
            logger.info(
                "Upserted %d standings rows for league_season_id=%s", inserted, league_season_id
            )
        logger.info("Standings update complete: %d rows in %.2fs", total_rows, time.time() - start)
        if return_summary:
            return {
                "total_rows": total_rows,
                "league_count": len(leagues_updated),
                "leagues_updated": leagues_updated,
                "unprocessed": unprocessed,
            }
        return total_rows
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update league standings from API.")
    parser.add_argument(
        "--league-season-ids", type=int, nargs="+",
        help="Refresh these league seasons (e.g. past seasons) instead of current ones.",
    )
    args = parser.parse_args()
    try:
        update_standings(league_season_ids=args.league_season_ids)
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received; shutting down standings update.")
