"""
Insert fixtures the API has but raw.raw_fixtures doesn't.

The daily update refreshes fixtures by the ids already in the table, so a
fixture created after a league season was bootstrapped never arrives: a
post-split phase (Veikkausliiga Championship Group), a season whose fixtures
were never loaded (J1 League 2027 after the move to an autumn-spring season),
a rescheduled replay with a new id.

Once a day, for every current league season: pull the season's fixtures (one
API call), and insert the ones whose api_fixture_id isn't in the table. Rows
already there are never touched: their fixture_id is referenced downstream
(forecasts, card versions), and the update-by-ids step owns their contents.
A fixture the API sends incomplete is skipped and logged; the next day's run
looks again, so no DLQ is needed.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import psycopg2.extras

from etl.src.config import UPDATE_FIXTURES_LOG
from etl.src.extract_fixtures import FIXTURES_ENDPOINT, extract_fixtures_field, fetch_fixtures
from etl.src.extract_metadata import get_db_connection
from etl.src.fixtures_dlq import fixture_row_problem
from etl.src.logger import get_logger
from etl.src.s3_landing import RAW_COLUMNS, RAW_TABLE

logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)

CURRENT_LEAGUE_SEASONS_SQL = """
    SELECT api_league_id, season
    FROM raw_int.int_league_context
    WHERE is_current
    ORDER BY api_league_id
"""

INSERT_SQL = f"""
    INSERT INTO {RAW_TABLE} ({', '.join(RAW_COLUMNS)})
    VALUES %s
    ON CONFLICT (api_fixture_id) DO NOTHING
"""


def current_league_seasons(conn) -> List[Tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute(CURRENT_LEAGUE_SEASONS_SQL)
        return [(int(lid), int(season)) for lid, season in cur.fetchall()]


def missing_fixtures(conn, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The records whose api_fixture_id isn't in the table yet (first one wins
    when the API repeats an id)."""
    by_id: Dict[int, Dict[str, Any]] = {}
    for record in records:
        api_id = record.get("api_fixture_id")
        if api_id is not None:
            by_id.setdefault(int(api_id), record)
    if not by_id:
        return []
    with conn.cursor() as cur:
        cur.execute(f"SELECT api_fixture_id FROM {RAW_TABLE} WHERE api_fixture_id = ANY(%s)",
                    (list(by_id),))
        have = {int(row[0]) for row in cur.fetchall()}
    return [record for api_id, record in by_id.items() if api_id not in have]


def insert_fixtures(conn, records: List[Dict[str, Any]]) -> int:
    """Insert new rows; a row that appeared meanwhile is left as it is."""
    if not records:
        return 0
    rows = [tuple(record.get(col) for col in RAW_COLUMNS) for record in records]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, INSERT_SQL, rows, page_size=1000)
        return cur.rowcount


def insert_new_fixtures(conn=None) -> Dict[str, Any]:
    """Find and insert missing fixtures for every current league season.

    Commits per league season, so one bad league doesn't undo the others.
    Raises when every league's API call failed or came back empty (bad or
    missing key, lapsed plan, outage), so the task goes red instead of
    reporting a quiet "nothing new".
    """
    own = conn is None
    conn = conn or get_db_connection()
    summary: Dict[str, Any] = {"league_seasons": 0, "api_fixtures": 0, "new": 0, "inserted": 0,
                               "skipped_invalid": 0, "failed": [], "inserted_by_league": {}}
    try:
        league_seasons = current_league_seasons(conn)
        summary["league_seasons"] = len(league_seasons)
        last_error: Optional[Exception] = None
        for api_league_id, season in league_seasons:
            try:
                payload = fetch_fixtures(FIXTURES_ENDPOINT, {"league": api_league_id, "season": season})
            except Exception as exc:
                last_error = exc
                summary["failed"].append(f"{api_league_id}/{season}")
                logger.error("new fixtures: fetch failed for league %s season %s: %s",
                             api_league_id, season, exc, exc_info=True)
                continue
            records = extract_fixtures_field(payload)
            summary["api_fixtures"] += len(records)
            try:
                new = missing_fixtures(conn, records)
                good = [r for r in new if fixture_row_problem(r) is None]
                skipped = len(new) - len(good)
                inserted = insert_fixtures(conn, good)
                conn.commit()
            except Exception as exc:
                conn.rollback()
                last_error = exc
                summary["failed"].append(f"{api_league_id}/{season}")
                logger.error("new fixtures: insert failed for league %s season %s: %s",
                             api_league_id, season, exc, exc_info=True)
                continue
            summary["new"] += len(new)
            summary["inserted"] += inserted
            summary["skipped_invalid"] += skipped
            if inserted:
                summary["inserted_by_league"][f"{api_league_id}/{season}"] = inserted
                logger.info("new fixtures: league %s season %s: inserted %d of %d from the API",
                            api_league_id, season, inserted, len(records))
            if skipped:
                logger.warning("new fixtures: league %s season %s: skipped %d incomplete fixture(s); "
                               "retried tomorrow", api_league_id, season, skipped)
        if league_seasons and len(summary["failed"]) == len(league_seasons):
            raise RuntimeError(
                f"new fixtures: all {len(league_seasons)} league seasons failed; last error: {last_error}"
            ) from last_error
        if league_seasons and summary["api_fixtures"] == 0:
            raise RuntimeError(f"new fixtures: the API returned no fixtures for any of "
                               f"{len(league_seasons)} current league seasons (key or plan?)")
    finally:
        if own:
            conn.close()
    logger.info("new fixtures: %s", {k: v for k, v in summary.items() if k != "inserted_by_league"})
    return summary
