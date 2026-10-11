"""
Ingest expected-goals (xG) per fixture from API-Football's fixtures/statistics.

Only leagues with verified provider coverage are enabled for scheduled ingestion.
The coverage probe can sample every tracked league without first adding it to the
allowlist. Stored xG is keyed by the shared fixture_id (same across raw_fixtures
and fact_head_to_head), so the engine can LEFT JOIN it onto historical matches
and blend into the Dixon-Coles fit.

Usage (run where the API + DB are reachable, e.g. the Airflow container):
    python -m etl.src.extract_xg              # backfill missing xG for covered leagues
    python -m etl.src.extract_xg --limit 40   # small test batch
    python -m etl.src.extract_xg --leagues 88 --seasons 2023,2024,2025
    python -m etl.src.extract_xg --probe-all-leagues --probe-sample-size 3
    python -m etl.src.extract_xg --coverage-report
"""

from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict
from typing import Iterable

import requests

from etl.src.config import API, API_HEADERS
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger

logger = get_logger(__name__)
API_URL = API["url"]
PAUSE = 0.3

# api_league_ids with verified usable xG coverage. The August 6 2026 probe used
# ten recent fixtures per candidate league: newly added leagues had >=80%
# complete xG coverage apart from the Eredivisie's 50% recent sample. 113 and
# 169 retain useful historical coverage even though their latest three-fixture
# probe was empty. Eredivisie coverage begins in 2023; older PSV fixtures return
# statistics but no expected_goals values.
XG_LEAGUES = {
    39, 40,                  # England: Premier League, Championship
    61,                      # France: Ligue 1
    71, 72,                  # Brazil: Serie A, Serie B
    78, 79,                  # Germany: Bundesliga, 2. Bundesliga
    88,                      # Netherlands: Eredivisie
    94,                      # Portugal: Primeira Liga
    103,                     # Norway: Eliteserien
    106,                     # Poland: Ekstraklasa
    113,                     # Sweden: Allsvenskan (historical/intermittent)
    119,                     # Denmark: Superliga
    135, 136,                # Italy: Serie A, Serie B
    140, 141,                # Spain: La Liga, Segunda Division
    144,                     # Belgium: Jupiler Pro League
    169,                     # China: Super League (historical/intermittent)
    179,                     # Scotland: Premiership
    203,                     # Turkey: Süper Lig
    207,                     # Switzerland: Super League
    233,                     # Egypt: Premier League
    253,                     # USA: Major League Soccer
    345,                     # Czech Republic: Czech Liga
    357,                     # Ireland: Premier Division
}

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

_PROBE_DDL = """
CREATE TABLE IF NOT EXISTS raw.xg_league_probes (
    api_league_id        BIGINT PRIMARY KEY,
    seasons              INTEGER[] NOT NULL,
    sampled_fixtures     INTEGER NOT NULL,
    xg_fixtures          INTEGER NOT NULL,
    complete_xg_fixtures INTEGER NOT NULL,
    sample_coverage_pct  NUMERIC NOT NULL,
    provider_supported   BOOLEAN NOT NULL,
    sample_fixture_ids   BIGINT[] NOT NULL,
    probed_at            TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_ATTEMPT_DDL = """
CREATE TABLE IF NOT EXISTS raw.fixture_xg_attempts (
    fixture_id     BIGINT PRIMARY KEY,
    api_fixture_id BIGINT,
    status         TEXT NOT NULL CHECK (status IN ('available', 'missing', 'error')),
    attempted_at   TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_COVERAGE_VIEW_DDL = """
CREATE OR REPLACE VIEW raw.xg_league_coverage AS
WITH completed AS (
    SELECT
        api_league_id,
        season,
        COUNT(*)::BIGINT AS completed_fixtures,
        MIN(kickoff_utc) AS first_completed_kickoff,
        MAX(kickoff_utc) AS last_completed_kickoff
    FROM raw.raw_fixtures
    WHERE fixture_status = 'FT'
    GROUP BY api_league_id, season
), covered AS (
    SELECT
        rf.api_league_id,
        rf.season,
        COUNT(x.fixture_id)::BIGINT AS xg_fixtures,
        COUNT(x.fixture_id) FILTER (
            WHERE x.home_xg IS NOT NULL AND x.away_xg IS NOT NULL
        )::BIGINT AS complete_xg_fixtures,
        MAX(x.captured_at) AS last_xg_captured_at
    FROM raw.raw_fixtures rf
    LEFT JOIN raw.fixture_xg x ON x.fixture_id = rf.fixture_id
    WHERE rf.fixture_status = 'FT'
    GROUP BY rf.api_league_id, rf.season
), attempts AS (
    SELECT
        rf.api_league_id,
        rf.season,
        COUNT(*) FILTER (WHERE a.status = 'missing')::BIGINT AS missing_xg_attempts,
        COUNT(*) FILTER (WHERE a.status = 'error')::BIGINT AS error_xg_attempts,
        MAX(a.attempted_at) AS last_xg_attempt_at
    FROM raw.raw_fixtures rf
    JOIN raw.fixture_xg_attempts a ON a.fixture_id = rf.fixture_id
    GROUP BY rf.api_league_id, rf.season
)
SELECT
    c.api_league_id,
    c.season,
    c.completed_fixtures,
    COALESCE(x.xg_fixtures, 0)::BIGINT AS xg_fixtures,
    COALESCE(x.complete_xg_fixtures, 0)::BIGINT AS complete_xg_fixtures,
    ROUND(
        100.0 * COALESCE(x.complete_xg_fixtures, 0)
        / NULLIF(c.completed_fixtures, 0),
        2
    ) AS coverage_pct,
    c.first_completed_kickoff,
    c.last_completed_kickoff,
    x.last_xg_captured_at,
    COALESCE(a.missing_xg_attempts, 0)::BIGINT AS missing_xg_attempts,
    COALESCE(a.error_xg_attempts, 0)::BIGINT AS error_xg_attempts,
    a.last_xg_attempt_at
FROM completed c
LEFT JOIN covered x
  ON x.api_league_id = c.api_league_id
 AND x.season = c.season
LEFT JOIN attempts a
  ON a.api_league_id = c.api_league_id
 AND a.season = c.season
"""


def bootstrap(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_DDL)
        cur.execute(_PROBE_DDL)
        cur.execute(_ATTEMPT_DDL)
        cur.execute(_COVERAGE_VIEW_DDL)
    conn.commit()


def _fetch_xg(api_fixture_id: int) -> dict:
    """{api_team_id: xg} for a fixture, or {} if none present."""
    r = requests.get(f"{API_URL}/fixtures/statistics",
                     params={"fixture": api_fixture_id}, headers=API_HEADERS, timeout=20)
    r.raise_for_status()
    payload = r.json()
    if payload.get("errors"):
        raise RuntimeError(f"API-Football returned errors for fixture {api_fixture_id}")
    out = {}
    for team in payload.get("response", []):
        tid = team.get("team", {}).get("id")
        for st in team.get("statistics", []):
            if st.get("type", "").lower() == "expected_goals" and st.get("value") is not None:
                try:
                    out[tid] = float(st["value"])
                except (TypeError, ValueError):
                    pass
    return out


def _upsert_xg(cur, fixture_id: int, api_fixture_id: int,
               home_xg: float | None, away_xg: float | None) -> None:
    cur.execute(
        """
        INSERT INTO raw.fixture_xg (fixture_id, api_fixture_id, home_xg, away_xg)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            api_fixture_id = EXCLUDED.api_fixture_id,
            home_xg = EXCLUDED.home_xg,
            away_xg = EXCLUDED.away_xg,
            captured_at = now()
        """,
        (fixture_id, api_fixture_id, home_xg, away_xg),
    )


def _record_attempt(cur, fixture_id: int, api_fixture_id: int, status: str) -> None:
    if status not in {"available", "missing", "error"}:
        raise ValueError(f"Unsupported xG attempt status: {status}")
    cur.execute(
        """
        INSERT INTO raw.fixture_xg_attempts (
            fixture_id, api_fixture_id, status, attempted_at
        )
        VALUES (%s, %s, %s, now())
        ON CONFLICT (fixture_id) DO UPDATE SET
            api_fixture_id = EXCLUDED.api_fixture_id,
            status = EXCLUDED.status,
            attempted_at = now()
        """,
        (fixture_id, api_fixture_id, status),
    )


def _normalise_ids(values: Iterable[int] | None, default: Iterable[int]) -> list[int]:
    selected = default if values is None else values
    return sorted({int(value) for value in selected})


def ingest(leagues=XG_LEAGUES, seasons=RECENT_SEASONS,
           only_missing: bool = True, limit: int | None = None) -> int:
    leagues = _normalise_ids(leagues, XG_LEAGUES)
    seasons = _normalise_ids(seasons, RECENT_SEASONS)
    conn = get_db_connection()
    bootstrap(conn)
    cur = conn.cursor()
    join = (
        "LEFT JOIN raw.fixture_xg x ON x.fixture_id = rf.fixture_id "
        "LEFT JOIN raw.fixture_xg_attempts a ON a.fixture_id = rf.fixture_id"
        if only_missing else ""
    )
    where_missing = (
        """AND x.fixture_id IS NULL
           AND (
               a.fixture_id IS NULL
               OR (a.status = 'missing' AND a.attempted_at < now() - interval '30 days')
               OR (a.status = 'error' AND a.attempted_at < now() - interval '1 day')
           )"""
        if only_missing else ""
    )
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
            _record_attempt(cur, fid, api_fid, "error")
            time.sleep(PAUSE)
            continue
        hx, ax = xg.get(home_tid), xg.get(away_tid)
        if hx is None and ax is None:
            _record_attempt(cur, fid, api_fid, "missing")
            time.sleep(PAUSE)
            continue
        _upsert_xg(cur, fid, api_fid, hx, ax)
        _record_attempt(cur, fid, api_fid, "available")
        stored += 1
        if stored % 50 == 0:
            conn.commit()
            logger.info("  stored %d...", stored)
        time.sleep(PAUSE)

    conn.commit()
    conn.close()
    logger.info("xG ingest complete: stored %d fixtures.", stored)
    return stored


def probe_league_coverage(
    leagues: Iterable[int] | None = None,
    seasons: Iterable[int] = RECENT_SEASONS,
    sample_size: int = 3,
    store_available: bool = False,
) -> list[dict]:
    """Sample recent FT fixtures per league and persist provider xG availability.

    This is intentionally separate from scheduled ingestion: probing discovers
    candidate leagues, while XG_LEAGUES remains the reviewed production allowlist.
    Available sampled values may optionally be stored to avoid wasting API reads.
    """
    if sample_size < 1:
        raise ValueError("sample_size must be at least 1")

    seasons = _normalise_ids(seasons, RECENT_SEASONS)
    league_filter = _normalise_ids(leagues, []) if leagues is not None else None
    conn = get_db_connection()
    bootstrap(conn)
    cur = conn.cursor()
    league_clause = "AND rf.api_league_id = ANY(%s)" if league_filter else ""
    params: list[object] = [seasons]
    if league_filter:
        params.append(league_filter)
    params.append(sample_size)
    cur.execute(
        f"""
        WITH candidates AS (
            SELECT
                rf.fixture_id,
                rf.api_fixture_id,
                rf.api_league_id,
                rf.home_team_id,
                rf.away_team_id,
                ROW_NUMBER() OVER (
                    PARTITION BY rf.api_league_id
                    ORDER BY rf.kickoff_utc DESC, rf.fixture_id DESC
                ) AS recency_rank
            FROM raw.raw_fixtures rf
            WHERE rf.fixture_status = 'FT'
              AND rf.season = ANY(%s)
              AND rf.api_fixture_id IS NOT NULL
              {league_clause}
        )
        SELECT fixture_id, api_fixture_id, api_league_id, home_team_id, away_team_id
        FROM candidates
        WHERE recency_rank <= %s
        ORDER BY api_league_id, recency_rank
        """,
        tuple(params),
    )
    grouped: dict[int, list[tuple]] = defaultdict(list)
    for row in cur.fetchall():
        grouped[int(row[2])].append(row)

    results = []
    for league_id, fixtures in grouped.items():
        xg_count = 0
        complete_count = 0
        sample_fixture_ids = []
        for fid, api_fid, _, home_tid, away_tid in fixtures:
            sample_fixture_ids.append(int(fid))
            try:
                xg = _fetch_xg(int(api_fid))
            except Exception as exc:  # noqa: BLE001
                logger.warning("xG probe failed for league=%s fixture=%s: %s",
                               league_id, api_fid, exc)
                time.sleep(PAUSE)
                continue
            hx, ax = xg.get(home_tid), xg.get(away_tid)
            if hx is not None or ax is not None:
                xg_count += 1
                if store_available:
                    _upsert_xg(cur, int(fid), int(api_fid), hx, ax)
            if hx is not None and ax is not None:
                complete_count += 1
            time.sleep(PAUSE)

        sampled = len(fixtures)
        pct = round(100.0 * complete_count / sampled, 2) if sampled else 0.0
        cur.execute(
            """
            INSERT INTO raw.xg_league_probes (
                api_league_id, seasons, sampled_fixtures, xg_fixtures,
                complete_xg_fixtures, sample_coverage_pct,
                provider_supported, sample_fixture_ids, probed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (api_league_id) DO UPDATE SET
                seasons = EXCLUDED.seasons,
                sampled_fixtures = EXCLUDED.sampled_fixtures,
                xg_fixtures = EXCLUDED.xg_fixtures,
                complete_xg_fixtures = EXCLUDED.complete_xg_fixtures,
                sample_coverage_pct = EXCLUDED.sample_coverage_pct,
                provider_supported = EXCLUDED.provider_supported,
                sample_fixture_ids = EXCLUDED.sample_fixture_ids,
                probed_at = now()
            """,
            (league_id, seasons, sampled, xg_count, complete_count, pct,
             xg_count > 0, sample_fixture_ids),
        )
        conn.commit()
        results.append({
            "api_league_id": league_id,
            "sampled_fixtures": sampled,
            "xg_fixtures": xg_count,
            "complete_xg_fixtures": complete_count,
            "sample_coverage_pct": pct,
            "provider_supported": xg_count > 0,
        })

    conn.close()
    results.sort(key=lambda row: (-row["sample_coverage_pct"], row["api_league_id"]))
    logger.info("xG coverage probe complete: %d leagues sampled.", len(results))
    return results


def coverage_report(
    seasons: Iterable[int] = RECENT_SEASONS,
    leagues: Iterable[int] | None = None,
) -> list[dict]:
    """Return current warehouse xG coverage at league-season grain."""
    seasons = _normalise_ids(seasons, RECENT_SEASONS)
    league_filter = _normalise_ids(leagues, []) if leagues is not None else None
    conn = get_db_connection()
    bootstrap(conn)
    cur = conn.cursor()
    league_clause = "AND c.api_league_id = ANY(%s)" if league_filter else ""
    params: list[object] = [seasons]
    if league_filter:
        params.append(league_filter)
    cur.execute(
        f"""
        SELECT
            c.api_league_id,
            COALESCE(l.league_name, 'Unknown') AS league_name,
            c.season,
            c.completed_fixtures,
            c.complete_xg_fixtures,
            c.coverage_pct,
            c.last_completed_kickoff,
            c.last_xg_captured_at,
            c.missing_xg_attempts,
            c.error_xg_attempts,
            c.last_xg_attempt_at,
            COALESCE(p.provider_supported, FALSE) AS provider_supported,
            c.api_league_id = ANY(%s) AS configured
        FROM raw.xg_league_coverage c
        LEFT JOIN dim.dim_leagues l ON l.api_league_id = c.api_league_id
        LEFT JOIN raw.xg_league_probes p ON p.api_league_id = c.api_league_id
        WHERE c.season = ANY(%s)
          {league_clause}
        ORDER BY c.coverage_pct DESC, c.api_league_id, c.season
        """,
        tuple([sorted(XG_LEAGUES), *params]),
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    conn.close()
    return rows


def log_coverage_summary(seasons: Iterable[int] = RECENT_SEASONS) -> list[dict]:
    rows = coverage_report(seasons=seasons, leagues=XG_LEAGUES)
    for row in rows:
        logger.info(
            "xG coverage league=%s season=%s: %s/%s (%s%%), last_capture=%s",
            row["api_league_id"], row["season"], row["complete_xg_fixtures"],
            row["completed_fixtures"], row["coverage_pct"], row["last_xg_captured_at"],
        )
    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest fixture xG from API-Football")
    ap.add_argument("--limit", type=int, default=None, help="max fixtures to fetch")
    ap.add_argument("--all", action="store_true", help="re-fetch even if already stored")
    ap.add_argument("--seasons", type=str, default=None,
                    help=f"comma-separated seasons (default: {RECENT_SEASONS})")
    ap.add_argument("--leagues", type=str, default=None,
                    help=f"comma-separated API league ids (default: {sorted(XG_LEAGUES)})")
    ap.add_argument("--probe-all-leagues", action="store_true",
                    help="sample recent FT fixtures in every tracked league")
    ap.add_argument("--probe-sample-size", type=int, default=3,
                    help="fixtures sampled per league during a provider probe")
    ap.add_argument("--store-probe-xg", action="store_true",
                    help="store xG values returned during a coverage probe")
    ap.add_argument("--coverage-report", action="store_true",
                    help="print current league-season warehouse xG coverage")
    args = ap.parse_args()
    seasons = ([int(s) for s in args.seasons.split(",")] if args.seasons else RECENT_SEASONS)
    leagues = ([int(s) for s in args.leagues.split(",")] if args.leagues else None)
    if args.probe_all_leagues:
        result = probe_league_coverage(
            leagues=leagues,
            seasons=seasons,
            sample_size=args.probe_sample_size,
            store_available=args.store_probe_xg,
        )
        print(json.dumps(result, indent=2, default=str))
    elif args.coverage_report:
        print(json.dumps(coverage_report(seasons=seasons, leagues=leagues),
                         indent=2, default=str))
    else:
        ingest(leagues=leagues or XG_LEAGUES, seasons=seasons,
               only_missing=not args.all, limit=args.limit)
