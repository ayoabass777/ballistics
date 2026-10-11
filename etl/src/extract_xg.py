"""
Ingest expected-goals (xG) per fixture from API-Football's fixtures/statistics.

Only a handful of leagues carry usable xG (see memory xg-data-coverage), so we
restrict to those to save API calls. Stored in raw.fixture_xg keyed by the shared
fixture_id (same across raw_fixtures and fact_head_to_head), so the engine can
LEFT JOIN it onto historical matches and blend into the Dixon-Coles fit.

Usage (run where the API + DB are reachable, e.g. the Airflow container):
    python -m etl.src.extract_xg              # backfill missing xG for covered leagues
    python -m etl.src.extract_xg --limit 40   # small test batch
"""

from __future__ import annotations

import argparse
import time

import requests

from etl.src.config import API, API_HEADERS
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger

logger = get_logger(__name__)
API_URL = API["url"]
PAUSE = 0.3

# api_league_ids with usable xG coverage (probe July 4 2026): Eliteserien, Brazil
# Serie B, Chinese Super League, Allsvenskan, Ireland Premier Division.
XG_LEAGUES = {103, 72, 169, 113, 357}

# Only recent seasons matter: Dixon-Coles time-decays history (half-life ~107 days),
# so older seasons contribute ~nothing to the fit and just burn API calls.
RECENT_SEASONS = [2025, 2026]

_DDL = """
CREATE TABLE IF NOT EXISTS raw.fixture_xg (
    fixture_id     BIGINT      PRIMARY KEY,
    api_fixture_id BIGINT,
    home_xg        NUMERIC,
    away_xg        NUMERIC,
    captured_at    TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def bootstrap(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_DDL)
    conn.commit()


def _fetch_xg(api_fixture_id: int) -> dict:
    """{api_team_id: xg} for a fixture, or {} if none present."""
    r = requests.get(f"{API_URL}/fixtures/statistics",
                     params={"fixture": api_fixture_id}, headers=API_HEADERS, timeout=20)
    out = {}
    for team in r.json().get("response", []):
        tid = team.get("team", {}).get("id")
        for st in team.get("statistics", []):
            if st.get("type", "").lower() == "expected_goals" and st.get("value") is not None:
                try:
                    out[tid] = float(st["value"])
                except (TypeError, ValueError):
                    pass
    return out


def ingest(leagues=XG_LEAGUES, seasons=RECENT_SEASONS,
           only_missing: bool = True, limit: int | None = None) -> int:
    conn = get_db_connection()
    bootstrap(conn)
    cur = conn.cursor()
    join = "LEFT JOIN raw.fixture_xg x ON x.fixture_id = rf.fixture_id" if only_missing else ""
    where_missing = "AND x.fixture_id IS NULL" if only_missing else ""
    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    cur.execute(
        f"""
        SELECT rf.fixture_id, rf.api_fixture_id, rf.home_team_id, rf.away_team_id
        FROM raw.raw_fixtures rf
        {join}
        WHERE rf.fixture_status = 'FT' AND rf.api_league_id = ANY(%s)
          AND rf.season = ANY(%s)
          AND rf.api_fixture_id IS NOT NULL
          {where_missing}
        ORDER BY rf.kickoff_utc DESC
        {limit_sql}
        """,
        (list(leagues), list(seasons)),
    )
    rows = cur.fetchall()
    logger.info("xG ingest: %d FT fixtures to fetch (leagues=%s)", len(rows), sorted(leagues))

    stored = 0
    for fid, api_fid, home_tid, away_tid in rows:
        try:
            xg = _fetch_xg(api_fid)
        except Exception as e:  # noqa: BLE001
            logger.error("xG fetch failed for fixture %s: %s", api_fid, e)
            time.sleep(PAUSE)
            continue
        hx, ax = xg.get(home_tid), xg.get(away_tid)
        if hx is None and ax is None:
            time.sleep(PAUSE)
            continue
        cur.execute(
            """
            INSERT INTO raw.fixture_xg (fixture_id, api_fixture_id, home_xg, away_xg)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fixture_id) DO UPDATE SET
                home_xg = EXCLUDED.home_xg, away_xg = EXCLUDED.away_xg, captured_at = now()
            """,
            (fid, api_fid, hx, ax),
        )
        stored += 1
        if stored % 50 == 0:
            conn.commit()
            logger.info("  stored %d...", stored)
        time.sleep(PAUSE)

    conn.commit()
    conn.close()
    logger.info("xG ingest complete: stored %d fixtures.", stored)
    return stored


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest fixture xG from API-Football")
    ap.add_argument("--limit", type=int, default=None, help="max fixtures to fetch")
    ap.add_argument("--all", action="store_true", help="re-fetch even if already stored")
    ap.add_argument("--seasons", type=str, default=None,
                    help=f"comma-separated seasons (default: {RECENT_SEASONS})")
    args = ap.parse_args()
    seasons = ([int(s) for s in args.seasons.split(",")] if args.seasons else RECENT_SEASONS)
    ingest(seasons=seasons, only_missing=not args.all, limit=args.limit)
