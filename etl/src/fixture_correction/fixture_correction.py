"""
Utility functions to correct fixture kickoff times for missed and changed fixtures.
- correct_missed_fixtures: looks back at recently played fixtures ("last" N).
- correct_changed_fixtures: looks ahead at upcoming fixtures ("next" N).
Both compare API kickoff times to DB values and update mismatches.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from etl.src.config import UPDATE_FIXTURES_LOG
from etl.src.extract_fixtures import fetch_fixtures, FIXTURES_ENDPOINT
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger

logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)


def _normalize_kickoff(dt_value: Any) -> Optional[datetime]:
    """
    Convert API/DB kickoff values to timezone-aware UTC datetime for comparison.
    Accepts datetime (with or without tz) or ISO string.
    """
    if dt_value is None:
        return None
    if isinstance(dt_value, datetime):
        if dt_value.tzinfo is None:
            return dt_value.replace(tzinfo=timezone.utc)
        return dt_value.astimezone(timezone.utc)
    if isinstance(dt_value, str):
        try:
            if dt_value.endswith("Z"):
                dt_value = dt_value.replace("Z", "+00:00")
            return datetime.fromisoformat(dt_value).astimezone(timezone.utc)
        except ValueError:
            logger.warning("Could not parse kickoff datetime string: %s", dt_value)
            return None
    return None


def get_league_seasons() -> List[Tuple[int, int]]:
    """
    Fetch current league_id/season pairs from int_league_context.
    """
    query = """
        SELECT l.api_league_id, season
        FROM raw_int.int_league_context l
        WHERE l.is_current
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(query)
        return cur.fetchall()
    finally:
        conn.close()


def get_db_kickoff_utc(fixture_id: int) -> Tuple[Optional[Any], Optional[str]]:
    """
    Retrieve kickoff time and status for a fixture.
    """
    query = """
        SELECT rf.kickoff_utc, rf.fixture_status
        FROM raw.raw_fixtures as rf
        WHERE rf.api_fixture_id = %s
        """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(query, (fixture_id,))
        result = cur.fetchone()
        if result:
            return result[0], result[1]
        return None, None
    except Exception as e:
        logger.error("Error fetching kickoff_utc for fixture_id %s: %s", fixture_id, e, exc_info=True)
        return None, None
    finally:
        conn.close()


def update_db_kickoff_utc(fixture_id: int, new_kickoff_utc: str) -> None:
    """
    Update kickoff time for a fixture.
    """
    query = """
        UPDATE raw.raw_fixtures
        SET kickoff_utc = %s, updated_at = NOW()
        WHERE api_fixture_id = %s
        """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(query, (new_kickoff_utc, fixture_id))
        conn.commit()
        logger.info("Successfully updated kickoff_utc for fixture_id %s to %s", fixture_id, new_kickoff_utc)
    except Exception as e:
        logger.error("Error updating kickoff_utc for fixture_id %s: %s", fixture_id, e, exc_info=True)
    finally:
        conn.close()


def _process_fixtures(fixtures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Compare API vs DB kickoff times and update DB where they differ.
    Returns list of updated fixtures metadata.
    """
    updated: List[Dict[str, Any]] = []
    for fixture in fixtures:
        api_fixture_id = fixture.get("fixture", {}).get("id")
        api_kickoff_utc = fixture.get("fixture", {}).get("date")
        db_kickoff_utc, db_status = get_db_kickoff_utc(api_fixture_id)

        api_dt = _normalize_kickoff(api_kickoff_utc)
        db_dt = _normalize_kickoff(db_kickoff_utc)

        if db_dt is None or db_status is None:
            logger.debug("Fixture %s not found in DB; skipping.", api_fixture_id)
            continue

        if (db_dt == api_dt) or db_status == "FT":
            logger.debug(
                "Fixture %s kickoff time matches or match is finished; no update needed.",
                api_fixture_id
            )
            continue

        update_db_kickoff_utc(api_fixture_id, api_kickoff_utc)
        updated.append(
            {
                "fixture_id": api_fixture_id,
                "old_kickoff_utc": db_kickoff_utc,
                "new_kickoff_utc": api_kickoff_utc,
            }
        )
        logger.info(
            "Updated fixture %s kickoff time from %s to %s.",
            api_fixture_id,
            db_kickoff_utc,
            api_kickoff_utc,
        )
    return updated


def correct_missed_fixtures(no_of_fixtures: int = 11) -> List[Dict[str, Any]]:
    """
    Check recently played fixtures (last N) for kickoff mismatches and update DB.
    """
    updated_fixtures: List[Dict[str, Any]] = []
    league_ids = get_league_seasons()
    for league_id, season in league_ids:
        past_fixtures = fetch_fixtures(
            FIXTURES_ENDPOINT,
            {
                "last": no_of_fixtures,
                "league": league_id,
                "season": season
            }
        )
        try:
            updated_fixtures.extend(_process_fixtures(past_fixtures))
        except Exception as e:
            logger.error("Error processing fetched fixtures for league %s: %s", league_id, e, exc_info=True)
            continue
    logger.info("Updated %d fixtures in correct_missed_fixtures.", len(updated_fixtures))
    return updated_fixtures


def correct_changed_fixtures(no_of_fixtures: int = 11) -> List[Dict[str, Any]]:
    """
    Check upcoming fixtures (next N) for kickoff mismatches and update DB.
    """
    updated_fixtures: List[Dict[str, Any]] = []
    league_ids = get_league_seasons()
    for league_id, season in league_ids:
        future_fixtures = fetch_fixtures(
            FIXTURES_ENDPOINT,
            {
                "next": no_of_fixtures,
                "league": league_id,
                "season": season
            }
        )
        try:
            updated_fixtures.extend(_process_fixtures(future_fixtures))
        except Exception as e:
            logger.error("Error processing fetched fixtures for league %s: %s", league_id, e, exc_info=True)
            continue
    logger.info("Updated %d fixtures in correct_changed_fixtures.", len(updated_fixtures))
    return updated_fixtures
