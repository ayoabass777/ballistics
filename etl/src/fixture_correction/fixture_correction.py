"""
Utility functions to correct unfinished fixtures against the provider.
- correct_missed_fixtures: looks back at recently played fixtures ("last" N).
- correct_changed_fixtures: looks ahead at upcoming fixtures ("next" N).
- reconcile_upcoming_fixtures: the stored upcoming window, plus orphans.
All compare API kickoff, status and home/away teams to DB values and update
mismatches (_process_fixtures).
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


TEAM_COLUMNS = ("home_team_id", "home_team_name", "away_team_id", "away_team_name")


def get_db_fixture_state(api_fixture_id: int) -> Optional[Dict[str, Any]]:
    """Kickoff, status and team sides stored for a fixture, or None if absent."""
    query = """
        SELECT rf.kickoff_utc, rf.fixture_status,
               rf.home_team_id, rf.home_team_name, rf.away_team_id, rf.away_team_name
        FROM raw.raw_fixtures as rf
        WHERE rf.api_fixture_id = %s
        """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(query, (api_fixture_id,))
        row = cur.fetchone()
        if not row:
            return None
        return dict(zip(("kickoff_utc", "fixture_status") + TEAM_COLUMNS, row))
    except Exception as e:
        logger.error("Error fetching state for fixture %s: %s", api_fixture_id, e, exc_info=True)
        return None
    finally:
        conn.close()


def update_db_fixture_state(fixture_id: int, kickoff_utc: Optional[str], status: str,
                            teams: Optional[Dict[str, Any]] = None) -> bool:
    """Persist the provider's current kickoff and status together for an
    unfinished fixture, and its team sides when `teams` is given."""
    teams = teams or {}
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE raw.raw_fixtures
            SET kickoff_utc = COALESCE(%s, kickoff_utc),
                fixture_status = %s,
                home_team_id = COALESCE(%s, home_team_id),
                home_team_name = COALESCE(%s, home_team_name),
                away_team_id = COALESCE(%s, away_team_id),
                away_team_name = COALESCE(%s, away_team_name),
                updated_at = NOW()
            WHERE api_fixture_id = %s AND fixture_status <> 'FT'
            """,
            (kickoff_utc, status, *(teams.get(c) for c in TEAM_COLUMNS), fixture_id),
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception:
        conn.rollback()
        logger.exception("Could not update fixture %s state", fixture_id)
        return False
    finally:
        conn.close()


def _api_teams(fixture: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The provider's home and away teams, or None unless both ids are present."""
    teams = fixture.get("teams") or {}
    home, away = teams.get("home") or {}, teams.get("away") or {}
    if home.get("id") is None or away.get("id") is None:
        return None
    return {"home_team_id": home["id"], "home_team_name": home.get("name"),
            "away_team_id": away["id"], "away_team_name": away.get("name")}


def _process_fixtures(fixtures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Compare API vs DB kickoff, status and team sides, and update the DB where
    they differ. Returns list of updated fixtures metadata.

    Team sides matter before kickoff: the provider sometimes swaps home and
    away after publishing a fixture (Livyi Bereh v Shakhtar Donetsk, 20 Nov
    2026), and the by-ID score update only reaches a fixture after it is
    played, so without this check the engine forecasts it the wrong way round.
    """
    updated: List[Dict[str, Any]] = []
    for fixture in fixtures:
        api_fixture_id = fixture.get("fixture", {}).get("id")
        api_kickoff_utc = fixture.get("fixture", {}).get("date")
        api_status = fixture.get("fixture", {}).get("status", {}).get("short")
        db = get_db_fixture_state(api_fixture_id)

        api_dt = _normalize_kickoff(api_kickoff_utc)
        db_dt = _normalize_kickoff(db["kickoff_utc"]) if db else None
        db_status = db["fixture_status"] if db else None

        if db_dt is None or db_status is None:
            logger.debug("Fixture %s not found in DB; skipping.", api_fixture_id)
            continue

        if db_status == "FT" or not isinstance(api_status, str) or not api_status:
            continue
        # FT without its score makes the fixture "played" with NULL goals,
        # which breaks Elo. The score updater writes FT and goals together.
        if api_status == "FT":
            continue
        api_teams = _api_teams(fixture)
        teams_changed = api_teams is not None and (
            (api_teams["home_team_id"], api_teams["away_team_id"])
            != (db["home_team_id"], db["away_team_id"]))
        if db_dt == api_dt and db_status == api_status and not teams_changed:
            continue
        if not update_db_fixture_state(api_fixture_id, api_kickoff_utc if api_dt else None,
                                       api_status, api_teams if teams_changed else None):
            continue
        change = {
            "fixture_id": api_fixture_id,
            "old_kickoff_utc": db["kickoff_utc"],
            "new_kickoff_utc": api_kickoff_utc,
            "old_status": db_status,
            "new_status": api_status,
        }
        if teams_changed:
            change["old_teams"] = f"{db['home_team_name']} v {db['away_team_name']}"
            change["new_teams"] = f"{api_teams['home_team_name']} v {api_teams['away_team_name']}"
            logger.warning("Fixture %s teams changed from %s to %s.",
                           api_fixture_id, change["old_teams"], change["new_teams"])
        updated.append(change)
        logger.info(
            "Updated fixture %s kickoff/status from %s/%s to %s/%s.",
            api_fixture_id, db["kickoff_utc"], db_status, api_kickoff_utc, api_status,
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


def _db_orphan_fixture_ids_in_window(league_id: int, season: int, days: int) -> List[str]:
    """Previously orphaned fixture IDs still dated inside the reconciliation window."""
    con = get_db_connection()
    cur = con.cursor()
    try:
        cur.execute(
            """
            SELECT api_fixture_id
            FROM raw.raw_fixtures
            WHERE api_league_id = %s AND season = %s
              AND fixture_status = 'ORPHAN'
              AND kickoff_utc >= NOW()
              AND kickoff_utc < NOW() + make_interval(days => %s)
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
      * ORPHANS — a DB NS fixture whose id is absent from the league schedule.
        Confirm each absence with a direct fixture-id lookup before excluding it;
        a partial schedule response must not remove a valid fixture.
      * RESTORATION — if a previously orphaned fixture reappears as NS in the
        league schedule, restore its status so the engine can analyze it again.

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

        # 1. date corrections and restoration for fixtures the API still lists
        updated.extend(_process_fixtures(api_fixtures))

        api_ids = {str(f.get("fixture", {}).get("id")) for f in api_fixtures
                   if f.get("fixture", {}).get("id") is not None}
        if not api_ids:
            logger.info("reconcile: league %s returned no API fixtures in window; skipping orphan check.", league_id)
            continue

        returned_ns_ids = {str(f["fixture"]["id"]) for f in api_fixtures
                           if f.get("fixture", {}).get("id") is not None
                           and f.get("fixture", {}).get("status", {}).get("short") == "NS"}
        restored = set(_db_orphan_fixture_ids_in_window(league_id, season, days)) & returned_ns_ids
        if restored:
            n = mark_fixtures_status(sorted(restored), "NS")
            logger.info("reconcile: league %s — restored %d orphaned fixtures to NS.", league_id, n)
            updated.extend({"fixture_id": oid, "action": "restored"} for oid in sorted(restored))

        # 2. Confirm each apparent orphan by fixture ID. Only a successful,
        # empty direct lookup is enough to remove an NS fixture.
        db_ids = set(_db_ns_fixture_ids_in_window(league_id, season, days))
        orphans = set()
        for oid in db_ids - api_ids:
            try:
                direct = fetch_fixtures(FIXTURES_ENDPOINT, {"id": int(oid)}, raise_on_failure=True)
            except Exception as exc:
                logger.warning("reconcile: could not verify fixture %s by id (%s); retaining NS.", oid, type(exc).__name__)
                continue
            if not direct:
                orphans.add(oid)
            else:
                updated.extend(_process_fixtures(direct))
        if orphans:
            n = mark_fixtures_status(sorted(orphans), ORPHAN_STATUS)
            logger.info("reconcile: league %s — marked %d orphaned fixtures %s.",
                        league_id, n, ORPHAN_STATUS)
            updated.extend({"fixture_id": oid, "action": "orphaned"} for oid in orphans)

    logger.info("Reconciled/flagged %d fixtures (window=%dd).", len(updated), days)
    return updated
