"""
Module to update played fixtures in raw.raw_fixtures by fetching missing results from the API.
- Groups league-season and fetches fixtures since earliest missing date.
- Updates home and away goals and fixture status for fixtures with null fulltime goals.
Optionally writes updates to JSON for backward compatibility.
"""

import argparse
import json
import os
import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from etl.src.config import UPDATE_FIXTURES_LOG, FIXTURE_UPDATES_JSON
from etl.src.extract_fixtures import fetch_fixtures, FIXTURES_ENDPOINT, extract_fixtures_field
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger

logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)


def _write_updates_json(updates: List[Dict[str, Any]], output_path: str) -> None:
    """
    Write the list of update dicts to a JSON file atomically.
    """
    tmp_path = output_path + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(updates, f, default=str, indent=2)
        os.replace(tmp_path, output_path)
        logger.info(f"Wrote {len(updates)} fixture updates to {output_path}")
    except (OSError, TypeError) as e:
        logger.error(f"Failed to write updates JSON at {output_path}: {e}", exc_info=True)
        raise


def to_json(updated_fixtures: List[Dict[str, Any]], output_filename: Optional[str]) -> int:
    """
    Write the list of fixture updates to a JSON file.
    """
    output_path = output_filename or FIXTURE_UPDATES_JSON
    _write_updates_json(updated_fixtures, output_path)
    return len(updated_fixtures)


def to_update_fixture_ids() -> List[Tuple[Optional[int], str]]:
    """Extract fixture_ids and api_fixture_ids that need updating."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT srf.fixture_id, srf.api_fixture_id
            FROM raw_stg.stg_raw_fixtures as srf
            JOIN raw_stg.stg_dim_leagues l
              ON srf.api_league_id = l.api_league_id
            JOIN raw_stg.stg_dim_league_seasons ls
              ON l.league_id = ls.league_id
             AND ls.season = srf.season
            WHERE srf.kickoff_utc < NOW() - INTERVAL '2 hour'
              AND (srf.home_team_fulltime_goal IS NULL OR srf.away_team_fulltime_goal IS NULL)
              AND ls.is_current
        """)
        rows = cur.fetchall()
        ids = [(row[0], str(row[1])) for row in rows]
        logger.info(f"Identified {len(ids)} fixture IDs needing updates.")
        return ids
    except Exception as e:
        logger.error(f"Error querying fixture IDs needing updates: {e}", exc_info=True)
        return []
    finally:
        conn.close()


def batch_update_fixtures(
    output_filename: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    write_output: bool = False,
) -> List[Dict[str, Any]]:
    """
    Batch-update fixtures in raw.raw_fixtures that lack fulltime goals by fetching
    updated data from the API, using the earliest missing kickoff date per league-season
    (or an explicit from_date).

    Returns list of updated fixtures (empty on error or no updates).
    """
    start_total = time.time()
    logger.info("Starting update_played_fixtures")
    updated_fixtures: List[Dict[str, Any]] = []
    if to_date is None:
        to_date = date.today().isoformat()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1) Determine league-season groups
        cur.execute("""
            SELECT
                ls.league_season_id,
                l.api_league_id,
                ls.season,
                MIN(srf.kickoff_utc)::date AS from_date
            FROM raw_stg.stg_raw_fixtures srf
            JOIN raw_stg.stg_dim_leagues l
                    ON srf.api_league_id = l.api_league_id
            JOIN raw_stg.stg_dim_league_seasons ls
              ON l.league_id = ls.league_id
              AND ls.season = srf.season
            WHERE
              srf.kickoff_utc < NOW() - INTERVAL '2 hour'
              AND (srf.home_team_fulltime_goal IS NULL OR srf.away_team_fulltime_goal IS NULL)
                And ls.is_current
            GROUP BY ls.league_season_id, l.api_league_id, ls.season
        """)
        to_update = cur.fetchall()
        if not to_update:
            logger.info("No fixtures need updating; exiting early.")
            if write_output or output_filename:
                to_json([], output_filename)
            return []
        logger.info(f"Found {len(to_update)} league-season groups to refresh")

        # 2) For each league-season, fetch fixtures since the earliest missing date
        for league_season_id, api_league_id, season, league_min_missing_date in to_update:
            start_req = time.time()
            fetch_from_date = from_date or league_min_missing_date.isoformat()
            try:
                fixtures = fetch_fixtures(
                    FIXTURES_ENDPOINT,
                    {
                        "league": api_league_id,
                        "season": season,
                        "from": fetch_from_date,
                        "to": to_date
                    }
                )
                fetch_fixtures_elapsed = time.time() - start_req
                logger.info(
                    "Fetched %d fixtures for league %s (API id %s), season %s in %.2fs",
                    len(fixtures),
                    league_season_id,
                    api_league_id,
                    season,
                    fetch_fixtures_elapsed,
                )
            except Exception as e:
                logger.error(
                    "Error fetching fixtures for league_season_id: %s (API id %s), season %s: %s",
                    league_season_id,
                    api_league_id,
                    season,
                    e,
                    exc_info=True
                )
                continue

            # 3) Extract updated fixtures batch
            try:
                extracted_batch = extract_fixtures_field(fixtures)
                updated_fixtures.extend(extracted_batch)
                logger.info(
                    "Extracted %d updated fixtures for league_season_id: %s (API id %s), season %s",
                    len(extracted_batch),
                    league_season_id,
                    api_league_id,
                    season,
                )
            except Exception as e:
                logger.error(
                    "Error extracting fixtures for league_season_id: %s (API id %s), season %s: %s",
                    league_season_id,
                    api_league_id,
                    season,
                    e,
                    exc_info=True
                )
                continue

            logger.info("Updated a total of %d in a batch of fixtures", len(extracted_batch))

        if not updated_fixtures:
            logger.info("No fixture updates to write; exiting early.")
            if write_output or output_filename:
                to_json([], output_filename)
            return []

        logger.info("Total of %d fixtures updated across all league-seasons.", len(updated_fixtures))
        if write_output or output_filename:
            to_json(updated_fixtures, output_filename)
        return updated_fixtures

    except Exception as e:
        logger.error(f"Error during update_played_fixtures: {e}", exc_info=True)
        return []

    finally:
        conn.close()
        total_elapsed = time.time() - start_total
        logger.info("Finished update_played_fixtures in %.2fs", total_elapsed)
        logger.info("Updated a total of %d fixtures.", len(updated_fixtures))


def update_by_ids(ids: List[Union[str, Tuple[Optional[int], str]]]) -> List[Dict[str, Any]]:
    """
    Update specific fixtures by their fixture IDs.
    """
    if not ids:
        logger.info("No fixture IDs provided for update; exiting early.")
        return []

    api_to_fixture: Dict[str, Optional[int]] = {}
    api_ids: List[str] = []
    for item in ids:
        if isinstance(item, tuple):
            fixture_id, api_id = item
            api_to_fixture[str(api_id)] = fixture_id
            api_ids.append(str(api_id))
        else:
            api_to_fixture[str(item)] = None
            api_ids.append(str(item))

    updated_fixtures: List[Dict[str, Any]] = []
    fixtures: List[Dict[str, Any]] = []
    logger.info("Starting update_by_ids for %d fixture IDs", len(api_ids))
    ptr = 0
    batch_size = 20  # Adjust batch size as needed
    while ptr < len(api_ids):
        batch_ids = api_ids[ptr : ptr + batch_size]
        batch_ids_str = "-".join(str(batch_id) for batch_id in batch_ids)
        logger.info(batch_ids_str)
        start_req = time.time()
        try:
            batch_fixtures = fetch_fixtures(
                FIXTURES_ENDPOINT,
                {
                    "ids": batch_ids_str
                }
            )
            fetch_fixtures_elapsed = time.time() - start_req
            logger.info(
                "Fetched %d fixtures for provided IDs in %.2fs",
                len(batch_fixtures),
                fetch_fixtures_elapsed,
            )
            fixtures.extend(batch_fixtures)
        except Exception as e:
            logger.error(
                "Error fetching fixtures for provided IDs: %s in batch %d",
                e,
                ptr // batch_size,
                exc_info=True
            )
        finally:
            ptr += batch_size

    try:
        extracted_batch = extract_fixtures_field(fixtures)
        if not extracted_batch:
            logger.info("No fixture updates extracted for provided IDs; exiting early.")
            return []

        for rec in extracted_batch:
            api_id = rec.get("api_fixture_id")
            fixture_id = api_to_fixture.get(str(api_id))
            if fixture_id is not None:
                rec["fixture_id"] = fixture_id
            updated_fixtures.append(rec)
        logger.info("Extracted %d updated fixtures for provided IDs", len(extracted_batch))
        return updated_fixtures
    except Exception as e:
        logger.error("Error extracting fixtures for provided IDs: %s", e, exc_info=True)
        return []


def correct_missed_fixtures() -> None:
    """
    Check for mismatch in fixtures kickoff times between API and DB, update DB if needed.
    """
    query = """
        SELECT l.api_league_id, season
        FROM raw_int.int_league_context l
        WHERE l.is_current
        """
    league_ids: List[Tuple[int, int]] = []
    no_of_fixtures = 11
    updated_fixtures = []
    con = get_db_connection()
    cur = con.cursor()
    cur.execute(query)
    league_ids = cur.fetchall()
    try:
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
                for fixture in past_fixtures:
                    api_fixture_id = fixture.get("fixture", {}).get("id")
                    api_kickoff_utc = fixture.get("fixture", {}).get("date")
                    db_kickoff_utc, db_status = get_db_kickoff_utc(api_fixture_id)
                    api_dt = _normalize_kickoff(api_kickoff_utc)
                    db_dt = _normalize_kickoff(db_kickoff_utc)

                    if db_dt is None or db_status is None:
                        logger.warning("Fixture %s not found in DB; skipping.", api_fixture_id)
                        continue

                    if (db_dt == api_dt) or db_status == "FT":
                        logger.info(
                            "Fixture %s kickoff time matches or match is finished; no update needed.",
                            api_fixture_id
                        )
                        continue

                    update_db_kickoff_utc(api_fixture_id, api_kickoff_utc)
                    updated_fixtures.append({
                        "fixture_id": api_fixture_id,
                        "old_kickoff_utc": db_kickoff_utc,
                        "new_kickoff_utc": api_kickoff_utc
                    })
                    logger.info(
                        "Updated fixture %s kickoff time from %s to %s for league_id %s.",
                        api_fixture_id,
                        db_kickoff_utc,
                        api_kickoff_utc,
                        league_id,
                    )
            except Exception as e:
                logger.error("Error processing fetched fixtures for league %s: %s", league_id, e, exc_info=True)

    except Exception as e:
        logger.error("Error correcting missed fixtures: %s", e, exc_info=True)

    finally:
        logger.info("Updated %d fixtures.", len(updated_fixtures))
        logger.info("Completed correcting missed fixtures.")
        con.close()


def correct_changed_fixtures() -> None:
    
    query = """
        SELECT l.api_league_id, season
        FROM raw_int.int_league_context l
        WHERE l.is_current
        """
    
    league_ids: List[Tuple[int, int]] = []
    no_of_fixtures = 20
    updated_fixtures = []
    con = get_db_connection()
    cur = con.cursor()
    cur.execute(query)
    league_ids = cur.fetchall()

    try: 
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
                for fixture in future_fixtures:
                    api_fixture_id = fixture.get("fixture", {}).get("id")
                    api_kickoff_utc = fixture.get("fixture", {}).get("date")
                    db_kickoff_utc, db_status = get_db_kickoff_utc(api_fixture_id)
                    api_dt = _normalize_kickoff(api_kickoff_utc)
                    db_dt = _normalize_kickoff(db_kickoff_utc)

                    if db_dt is None or db_status is None:
                        logger.warning("Fixture %s not found in DB; skipping.", api_fixture_id)
                        continue

                    if (db_dt == api_dt) or db_status == "FT":
                        logger.info(
                            "Fixture %s kickoff time matches; no update needed.",
                            api_fixture_id
                        )
                        continue

                    update_db_kickoff_utc(api_fixture_id, api_kickoff_utc)
                    updated_fixtures.append({
                        "fixture_id": api_fixture_id,
                        "old_kickoff_utc": db_kickoff_utc,
                        "new_kickoff_utc": api_kickoff_utc
                    })
                    logger.info(
                        "Updated fixture %s kickoff time from %s to %s for league_id %s.",
                        api_fixture_id,
                        db_kickoff_utc,
                        api_kickoff_utc,
                        league_id,
                    )
            
            except Exception as e:
                logger.error("Error processing fetched fixtures for league %s: %s", league_id, e, exc_info=True)
    except Exception as e:
        logger.error("Error correcting fixture kickoff times: %s", e, exc_info=True)

    finally:
        logger.info("Updated %d fixtures.", len(updated_fixtures))
        logger.info("Completed correcting fixture kickoff times.")
        con.close()


def get_db_kickoff_utc(fixture_id: int) -> Tuple[Optional[str], Optional[str]]:
    """
    Retrieve kickoff time and status for a fixture.
    """
    query = """
        SELECT rf.kickoff_utc, rf.fixture_status
        FROM raw.raw_fixtures as rf
        WHERE rf.api_fixture_id = %s
        """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

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
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, (new_kickoff_utc, fixture_id))
        conn.commit()
        logger.info("Successfully updated kickoff_utc for fixture_id %s to %s", fixture_id, new_kickoff_utc)
    except Exception as e:
        logger.error("Error updating kickoff_utc for fixture_id %s: %s", fixture_id, e, exc_info=True)
    finally:
        conn.close()


def _normalize_kickoff(dt_value: Any) -> Optional[datetime]:
    """
    Convert API/db kickoff values to timezone-aware UTC datetime for comparison.
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
            # Support trailing Z
            if dt_value.endswith("Z"):
                dt_value = dt_value.replace("Z", "+00:00")
            return datetime.fromisoformat(dt_value).astimezone(timezone.utc)
        except ValueError:
            logger.warning("Could not parse kickoff datetime string: %s", dt_value)
            return None
    return None


def update_fixtures_main(
    output_filename: Optional[str] = None,
    write_output: bool = False
) -> List[Dict[str, Any]]:
    """
    Main function to update played fixtures and return updates.
    """
    try:
        ids = to_update_fixture_ids()
        updated_fixtures = update_by_ids(ids)
        if write_output or output_filename:
            to_json(updated_fixtures, output_filename)
        logger.info("update_fixtures_main finished: %d updates applied.", len(updated_fixtures))
        return updated_fixtures
    except Exception as e:
        logger.error(f"Error in update_fixtures_main: {e}", exc_info=True)
        return []
    finally:
        logger.info("update_fixtures_main completed.")


def batch_update_main(
    output_filename: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    write_output: bool = False
) -> List[Dict[str, Any]]:
    """
    Main function to batch update fixtures (league-season grouping) and return updates.
    """
    try:
        updated_fixtures = batch_update_fixtures(
            output_filename=output_filename,
            from_date=from_date,
            to_date=to_date,
            write_output=write_output or bool(output_filename),
        )
        logger.info("batch_update_main finished: %d updates applied.", len(updated_fixtures))
        return updated_fixtures
    except Exception as e:
        logger.error(f"Error in batch_update_main: {e}", exc_info=True)
        return []
    finally:
        logger.info("batch_update_main completed.")


def rescheduled_fixtures_main() -> None:
    """
    Main function to correct missed fixtures kickoff times.
    """
    try:
        correct_missed_fixtures()
    except Exception as e:
        logger.error(f"Error in rescheduled_fixtures_main: {e}", exc_info=True)
    finally:
        logger.info("rescheduled_fixtures_main completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch played fixture updates."
    )
    parser.add_argument(
        "--earliest-kickoff",
        action="store_true",
        help="If set, update fixtures by earliest missing kickoff per league-season (default path updates by specific IDs)."
    )
    parser.add_argument(
        "--output-file",
        dest="output_file",
        type=str,
        help="Path to write the updates JSON (defaults to FIXTURE_UPDATES_JSON)."
    )
    parser.add_argument(
        "--write-output",
        action="store_true",
        help="Write fixture updates to JSON (defaults to off unless output_file is provided)."
    )
    parser.add_argument(
        "--to-date",
        dest="to_date",
        type=str,
        help="ISO date (YYYY-MM-DD) to fetch fixtures up to (defaults to today)."
    )
    parser.add_argument(
        "--from-date",
        dest="from_date",
        type=str,
        help="ISO date (YYYY-MM-DD) to fetch fixtures from (defaults to earliest missing per league-season)."
    )
    parser.add_argument(
        "--rescheduled",
        action="store_true",
        help="If set, correct rescheduled fixtures kickoff times."
    )
    args = parser.parse_args()

    logger.info(
        "Invoking batch fixture updates (from_date=%s, to_date=%s, output_file=%s)",
        args.from_date,
        args.to_date,
        args.output_file,
    )
    try:
        if args.earliest_kickoff:
            updated_fixtures = batch_update_fixtures(
                output_filename=args.output_file,
                from_date=args.from_date,
                to_date=args.to_date,
                write_output=args.write_output or bool(args.output_file)
            )
            logger.info("batch_update_fixtures finished: %d updates applied.", len(updated_fixtures))

        elif args.rescheduled:
            correct_changed_fixtures()
            logger.info("correct_missed_fixtures finished.")

        else:
            update_fixtures_main(
                output_filename=args.output_file,
                write_output=args.write_output or bool(args.output_file)
            )

    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received: shutting down update_played_fixtures gracefully.")
