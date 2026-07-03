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

FIXTURE_CORRECTION_BOUND = 11


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


def correct_missed_fixtures(no_of_fixtures: int = FIXTURE_CORRECTION_BOUND) -> List[Dict[str, Any]]:
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


def correct_changed_fixtures(no_of_fixtures: int = FIXTURE_CORRECTION_BOUND) -> List[Dict[str, Any]]:
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


RECONCILE_WINDOW_DAYS = 14
ORPHAN_STATUS = "ORPHAN"   # DB fixture the API no longer lists (superseded/removed)


def _db_ns_fixture_ids_in_window(league_id: int, season: int, days: int) -> List[str]:
    """api_fixture_ids of this league-season's NS fixtures stored in [now, now+days]."""
    con = get_db_connection()
    cur = con.cursor()
    try:
        cur.execute(
            """
            SELECT api_fixture_id
            FROM raw.raw_fixtures
            WHERE api_league_id = %s AND season = %s
              AND fixture_status = 'NS'
              AND kickoff_utc >= NOW()
              AND kickoff_utc <  NOW() + make_interval(days => %s)
              AND api_fixture_id IS NOT NULL
            """,
            (league_id, season, days),
        )
        return [str(r[0]) for r in cur.fetchall()]
    finally:
        con.close()


def mark_fixtures_status(api_fixture_ids: List[str], status: str) -> int:
    """Set fixture_status for the given api_fixture_ids. Returns rows affected."""
    if not api_fixture_ids:
        return 0
    con = get_db_connection()
    cur = con.cursor()
    try:
        cur.execute(
            """
            UPDATE raw.raw_fixtures
            SET fixture_status = %s, updated_at = NOW()
            WHERE api_fixture_id = ANY(%s)
            """,
            (status, [int(i) for i in api_fixture_ids]),
        )
        con.commit()
        return cur.rowcount
    except Exception as e:
        logger.error("Error marking fixtures %s as %s: %s", api_fixture_ids[:3], status, e, exc_info=True)
        con.rollback()
        return 0
    finally:
        con.close()


def reconcile_upcoming_fixtures(days: int = RECONCILE_WINDOW_DAYS) -> List[Dict[str, Any]]:
    """Set-reconcile each current league-season's upcoming fixtures against the
    API's authoritative list for the window.

    Closes two blind spots the ``next N``/``last N`` correction functions miss:
      * date drift — corrected via _process_fixtures on the API's returned set;
      * ORPHANS — a DB NS fixture stuck on a stale future date whose id the API no
        longer lists (a provisional round replaced by new fixture ids). Fetch-by-id
        returns nothing for these, so per-id checks can't see them; here we diff the
        DB's NS set against the API's set and mark the leftovers ORPHAN, dropping
        them from every NS-based query (e.g. the betting pool).

    Guard: orphan-marking only runs when the API returned ≥1 fixture for the
    league-season, so a transient empty API response never nukes real fixtures.
    """
    updated: List[Dict[str, Any]] = []
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    frm = now.strftime("%Y-%m-%d")
    to = (now + timedelta(days=days)).strftime("%Y-%m-%d")

    for league_id, season in get_league_seasons():
        try:
            api_fixtures = fetch_fixtures(
                FIXTURES_ENDPOINT,
                {"league": league_id, "season": season, "from": frm, "to": to},
            )
        except Exception as e:
            logger.error("reconcile: API fetch failed for league %s: %s", league_id, e, exc_info=True)
            continue

        # 1. date corrections on the fixtures the API still lists
        updated.extend(_process_fixtures(api_fixtures))

        api_ids = {str(f.get("fixture", {}).get("id")) for f in api_fixtures
                   if f.get("fixture", {}).get("id") is not None}
        if not api_ids:
            logger.info("reconcile: league %s returned no API fixtures in window; skipping orphan check.", league_id)
            continue

        # 2. orphans: DB NS fixtures in window the API no longer lists
        db_ids = set(_db_ns_fixture_ids_in_window(league_id, season, days))
        orphans = db_ids - api_ids
        if orphans:
            n = mark_fixtures_status(list(orphans), ORPHAN_STATUS)
            logger.info("reconcile: league %s — marked %d orphaned fixtures %s.",
                        league_id, n, ORPHAN_STATUS)
            updated.extend({"fixture_id": oid, "action": "orphaned"} for oid in orphans)

    logger.info("Reconciled/flagged %d fixtures (window=%dd).", len(updated), days)
    return updated
