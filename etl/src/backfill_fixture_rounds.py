"""
Backfill raw.raw_fixtures.round for fixtures loaded before 2026-10-12.

One API call per (api_league_id, season) that still has fixtures with a NULL
round. Only `round` (and updated_at, so incremental dbt picks the rows up) is
written; scores, ids and statuses are left alone. Resumable: a rerun skips
seasons already filled. The untouched API payload is landed in S3 when
configured, so the next missing field doesn't need another pull.

    python -m etl.src.backfill_fixture_rounds --dry-run
    python -m etl.src.backfill_fixture_rounds [--limit N] [--no-s3]
"""
import argparse
import os
from datetime import date
from typing import Any, Dict, List, Tuple

from psycopg2.extras import execute_values

from etl.src.extract_fixtures import FIXTURES_ENDPOINT, fetch_fixtures
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger
from etl.src.schema_checks import migrate_raw_fixtures_columns

logger = get_logger(__name__)

PENDING_SQL = """
    SELECT api_league_id, season, count(*) AS n_missing
    FROM raw.raw_fixtures
    WHERE round IS NULL
    GROUP BY api_league_id, season
    ORDER BY season DESC, api_league_id
"""

UPDATE_SQL = """
    UPDATE raw.raw_fixtures AS rf
    SET round = v.round, updated_at = NOW()
    FROM (VALUES %s) AS v(api_fixture_id, round)
    WHERE rf.api_fixture_id = v.api_fixture_id
      AND rf.round IS DISTINCT FROM v.round
"""


def rounds_from_payload(payload: List[Dict[str, Any]]) -> List[Tuple[int, str]]:
    """(api_fixture_id, round) for every fixture that has both."""
    out: Dict[int, str] = {}
    for item in payload or []:
        fid = (item.get("fixture") or {}).get("id")
        rnd = (item.get("league") or {}).get("round")
        if fid is not None and rnd:
            out[int(fid)] = rnd
    return sorted(out.items())


def pending_seasons(cur) -> List[Tuple[int, int, int]]:
    cur.execute(PENDING_SQL)
    return cur.fetchall()


def backfill(limit: int | None = None, dry_run: bool = False, land_s3: bool = True) -> Dict[str, int]:
    conn = get_db_connection()
    cur = conn.cursor()
    summary = {"seasons": 0, "api_calls": 0, "updated": 0, "still_missing": 0}
    try:
        migrate_raw_fixtures_columns(cur)
        conn.commit()
        seasons = pending_seasons(cur)
        summary["seasons"] = len(seasons)
        logger.info("%d league seasons have fixtures without a round (%d fixtures).",
                    len(seasons), sum(n for _, _, n in seasons))
        if dry_run:
            for api_league_id, season, n in seasons:
                logger.info("pending: league %s season %s (%d fixtures)", api_league_id, season, n)
            return summary

        ds = date.today().isoformat()
        for api_league_id, season, n_missing in seasons[:limit] if limit else seasons:
            payload = fetch_fixtures(FIXTURES_ENDPOINT, params={"league": api_league_id, "season": season})
            summary["api_calls"] += 1
            if land_s3 and payload:
                try:
                    from etl.src.s3_landing import write_raw_payload_to_s3
                    write_raw_payload_to_s3(payload, api_league_id, season, ds)
                except Exception as e:  # landing is a bonus, never block the backfill
                    logger.warning("Raw payload landing failed for %s/%s: %s", api_league_id, season, e)
            pairs = rounds_from_payload(payload)
            if pairs:
                execute_values(cur, UPDATE_SQL, pairs, page_size=len(pairs))  # one statement, so rowcount is the total
                updated = cur.rowcount or 0
                conn.commit()
            else:
                updated = 0
            summary["updated"] += updated
            logger.info("league %s season %s: %d of %d fixtures got a round",
                        api_league_id, season, updated, n_missing)

        cur.execute("SELECT count(*) FROM raw.raw_fixtures WHERE round IS NULL")
        summary["still_missing"] = cur.fetchone()[0]
        logger.info("Backfill done: %s", summary)
        return summary
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="List pending seasons; no API calls.")
    parser.add_argument("--limit", type=int, help="Only the first N pending seasons (newest first).")
    parser.add_argument("--no-s3", action="store_true", help="Skip raw payload landing.")
    args = parser.parse_args()
    land = not args.no_s3 and bool(os.getenv("AWS_ACCESS_KEY_ID"))
    print(backfill(limit=args.limit, dry_run=args.dry_run, land_s3=land))
