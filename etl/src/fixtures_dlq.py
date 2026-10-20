"""
Fixtures with unprocessable rows: row-level DLQ on S3 (D148), same policy as
standings (D147).

The fixture DLQ that already existed (dlq/{league}/{season}/{ds}/error.json)
catches whole failed extractions. This one catches single bad rows in a good
response, which used to go into the bulk COPY and could fail the whole league
season's load:

  validate    fixture_row_problem: no api_fixture_id (the key), no kickoff,
              no status, or no home / away team id
  park        s3://<bucket>/dlq/fixtures/rows/{api_league_id}/{season}/open.json
  load        the good rows replace their own fixtures only (the league's
              other rows, including the parked fixture's last good row, stay);
              a clean load replaces the whole league season as before
  flag        raw.raw_fixtures_status (one row per league season)
  replay      hourly (replay_dag): re-pull the league; clean → resolved,
              still bad → attempts + 1; dead after 24 + alert. No nightly
              run reloads fixtures by league, so dead entries are rechecked
              once a day instead (dlq.dead_recheck_due)
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from etl.src import dlq
from etl.src.data_detector.monitoring import monitored
from etl.src.logger import get_logger

logger = get_logger(__name__)

PREFIX = "dlq/fixtures/rows/"
STATUS_TABLE = "raw.raw_fixtures_status"

STATUS_DDL = f"""
CREATE TABLE IF NOT EXISTS {STATUS_TABLE} (
    api_league_id      INT NOT NULL,
    season             INT NOT NULL,
    status             TEXT NOT NULL,          -- ok | unprocessed | dead
    unprocessed_rows   INT NOT NULL DEFAULT 0,
    unprocessed_fixtures TEXT[],
    reason             TEXT,
    unprocessed_since  TIMESTAMPTZ,
    replay_attempts    INT NOT NULL DEFAULT 0,
    dlq_key            TEXT,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (api_league_id, season)
);
"""


CLEARS = ("Rechecked once a day: it resolves itself when the API sends the rows clean. "
          "To force it, re-run bootstrap for that league season.")


def dlq_key(api_league_id: int, season: int) -> str:
    return f"{PREFIX}{api_league_id}/{season}/open.json"


def fixture_row_problem(row: Dict[str, Any]) -> Optional[str]:
    """Why a fixture row can't be loaded, or None when it can."""
    if row.get("api_fixture_id") is None:
        return "null_fixture_id"         # the key (unique index)
    if not row.get("kickoff_utc"):
        return "null_kickoff"            # NOT NULL; every model keys on it
    if not row.get("fixture_status"):
        return "null_status"             # NOT NULL
    if row.get("home_team_id") is None or row.get("away_team_id") is None:
        return "null_team_id"            # joins to teams, Elo, patterns
    return None


def _label(row: Dict[str, Any]) -> str:
    return (f"{row.get('home_team_name')} v {row.get('away_team_name')}"
            f" ({row.get('api_fixture_id') or 'no id'})")


def park_fixture_rows(bad: List[Dict[str, Any]], *, league: Dict[str, Any], ds: str,
                      source: str = "bootstrap", **_: Any) -> Optional[tuple]:
    """Write a league season's bad fixture rows to its open entry; a replay
    that still finds them counts as a failed attempt. (key, entry, created),
    or None when S3 is unreachable."""
    key = dlq_key(league["api_league_id"], league["season"])
    reasons = sorted({r[dlq.DLQ_REASON] for r in bad})
    try:
        existing = dlq.read_json(key)
        created = existing is None
        entry = existing or {"kind": "fixture_rows", "api_league_id": league["api_league_id"],
                             "season": league["season"], **dlq.new_state(), "ds_seen": []}
        entry.update(rows=bad, reasons=reasons, fixtures=[_label(r) for r in bad],
                     last_seen=dlq._iso(dlq.now_utc()),
                     ds_seen=sorted(set(entry.get("ds_seen") or []) | {str(ds)}))
        became_dead = False
        if source == "replay" and not created:   # a new entry starts at 0 attempts
            entry, became_dead = dlq.record_failed_attempt(entry, f"still unprocessable: {', '.join(reasons)}")
        dlq.write_json(key, entry)
    except Exception as exc:
        logger.error("fixture row DLQ write failed for %s: %s", key, exc, exc_info=True)
        return None
    logger.warning("league %s season %s: parked %d fixture row(s) (%s) at s3://%s/%s",
                   league["api_league_id"], league["season"], len(bad),
                   ", ".join(entry["fixtures"]), dlq.default_bucket(), key)
    if became_dead:
        dlq.alert_dead("fixture rows", key, entry,
                       f"Fixtures for league {league['api_league_id']} season {league['season']} "
                       f"({', '.join(entry['fixtures'])})", clears=CLEARS)
    return key, entry, created


def has_open_entry(league: Dict[str, Any]) -> bool:
    try:
        return dlq.read_json(dlq_key(league["api_league_id"], league["season"])) is not None
    except Exception as exc:
        logger.error("fixture row DLQ read failed for %s: %s", league, exc)
        return False


def resolve_open_entry(league: Dict[str, Any], how: str) -> Optional[str]:
    key = dlq_key(league["api_league_id"], league["season"])
    try:
        entry = dlq.read_json(key)
        if entry is None:
            return None
        return dlq.resolve_entry(key, entry, how)
    except Exception as exc:
        logger.error("fixture row DLQ resolve failed for %s: %s", key, exc, exc_info=True)
        return None


def set_status(league: Dict[str, Any], bad: List[Dict[str, Any]],
               parked: Optional[tuple], conn=None) -> None:
    from etl.src.extract_metadata import get_db_connection
    entry = parked[1] if parked else {}
    status = "ok" if not bad else ("dead" if entry.get("status") == dlq.DEAD else "unprocessed")
    own = conn is None
    conn = conn or get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(STATUS_DDL)
            cur.execute(
                f"""
                INSERT INTO {STATUS_TABLE} AS s
                    (api_league_id, season, status, unprocessed_rows, unprocessed_fixtures, reason,
                     unprocessed_since, replay_attempts, dlq_key, updated_at)
                VALUES (%(lid)s, %(season)s, %(status)s, %(n)s, %(fixtures)s, %(reason)s,
                        CASE WHEN %(status)s = 'ok' THEN NULL ELSE now() END, %(attempts)s, %(key)s, now())
                ON CONFLICT (api_league_id, season) DO UPDATE SET
                    status = EXCLUDED.status,
                    unprocessed_rows = EXCLUDED.unprocessed_rows,
                    unprocessed_fixtures = EXCLUDED.unprocessed_fixtures,
                    reason = EXCLUDED.reason,
                    unprocessed_since = CASE WHEN EXCLUDED.status = 'ok' THEN NULL
                                             ELSE COALESCE(s.unprocessed_since, EXCLUDED.unprocessed_since) END,
                    replay_attempts = EXCLUDED.replay_attempts,
                    dlq_key = EXCLUDED.dlq_key,
                    updated_at = now()
                """,
                {"lid": league["api_league_id"], "season": league["season"], "status": status,
                 "n": len(bad), "fixtures": [_label(r) for r in bad] or None,
                 "reason": ", ".join(sorted({r[dlq.DLQ_REASON] for r in bad})) or None,
                 "attempts": int(entry.get("attempts") or 0), "key": parked[0] if parked else None},
            )
        if own:
            conn.commit()
    except Exception:
        if own:
            conn.rollback()
        raise
    finally:
        if own:
            conn.close()


@monitored(dataset="fixtures", target_table="raw.raw_fixtures")
@dlq.dead_letter(validate=fixture_row_problem, park=park_fixture_rows)
def load_league_fixtures(rows: List[Dict[str, Any]], *, league: Dict[str, Any], ds: str,
                         conn, source: str = "bootstrap", quarantined=(), parked=None) -> dlq.LoadResult:
    """Load a league season's good rows in the caller's transaction: COPY into
    a staging table, then upsert on api_fixture_id, so a fixture already in
    the table keeps its fixture_id (forecasts, card versions, the consensus
    pool and the facts key on it). A clean load also removes the league
    season's rows the API no longer sends; with rows parked nothing is
    removed, so a parked fixture keeps its last good row."""
    from etl.src.s3_landing import RAW_COLUMNS, RAW_TABLE, _records_to_buffer
    cols = ", ".join(RAW_COLUMNS)
    updates = ", ".join(f"{c} = l.{c}" for c in RAW_COLUMNS if c != "api_fixture_id")
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE TEMP TABLE fixtures_load ON COMMIT DROP AS SELECT {cols} FROM {RAW_TABLE} WITH NO DATA")
        count = 0
        if rows:
            buffer, count = _records_to_buffer(rows)
            cur.copy_expert(f"COPY fixtures_load ({cols}) FROM STDIN WITH CSV NULL ''", buffer)
        # Update, then insert only new ids: an INSERT ... ON CONFLICT DO UPDATE
        # would draw a fixture_id from the sequence for every existing row.
        cur.execute("DELETE FROM fixtures_load a USING fixtures_load b "
                    "WHERE a.api_fixture_id = b.api_fixture_id AND a.ctid < b.ctid")   # API repeats: last wins
        cur.execute(
            f"""UPDATE {RAW_TABLE} r SET {updates}, updated_at = now()
                FROM fixtures_load l WHERE r.api_fixture_id = l.api_fixture_id""")
        cur.execute(
            f"""INSERT INTO {RAW_TABLE} ({cols})
                SELECT {cols} FROM fixtures_load l
                WHERE NOT EXISTS (SELECT 1 FROM {RAW_TABLE} r WHERE r.api_fixture_id = l.api_fixture_id)""")
        if not quarantined:
            cur.execute(
                f"""DELETE FROM {RAW_TABLE} r
                    WHERE r.api_league_id = %s AND r.season = %s
                      AND NOT EXISTS (SELECT 1 FROM fixtures_load l WHERE l.api_fixture_id = r.api_fixture_id)""",
                (league["api_league_id"], league["season"]))
        cur.execute("DROP TABLE fixtures_load")
    finally:
        cur.close()
    resolved = False
    if not quarantined:             # resolve the open entry only once the caller commits
        resolved = has_open_entry(league)
        if resolved:
            dlq.after_commit(conn, lambda: resolve_open_entry(league, how=source))
    cur = conn.cursor()             # the flag commits (or rolls back) with the rows
    try:
        cur.execute("SAVEPOINT fixtures_status")
        try:
            set_status(league, list(quarantined), parked, conn=conn)
            cur.execute("RELEASE SAVEPOINT fixtures_status")
        except Exception as exc:    # the flag must not undo the load
            cur.execute("ROLLBACK TO SAVEPOINT fixtures_status")
            logger.error("fixture status update failed for %s: %s", league, exc)
    finally:
        cur.close()
    return dlq.LoadResult(loaded=count, parked=list(quarantined),
                          dlq_key=parked[0] if parked else None,
                          new_entry=bool(parked and parked[2]), resolved=resolved,
                          dlq_write_failed=bool(quarantined) and not parked)


def _failed_attempt(key: str, entry: Dict[str, Any], error: str) -> None:
    entry, became_dead = dlq.record_failed_attempt(entry, error)
    dlq.write_json(key, entry)
    league = {"api_league_id": entry["api_league_id"], "season": entry["season"]}
    set_status(league, entry.get("rows") or [], (key, entry))
    if became_dead:
        dlq.alert_dead("fixture rows", key, entry,
                       f"Fixtures for league {entry['api_league_id']} season {entry['season']}", clears=CLEARS)


def replay_fixture_rows_dlq(ds: Optional[str] = None) -> Dict[str, int]:
    """Retry every due fixture-row entry: re-pull the league season, land it
    on S3 and load it through the same validator."""
    from etl.src.extract_fixtures import FIXTURES_ENDPOINT, extract_fixtures_field, fetch_fixtures
    from etl.src.extract_metadata import get_db_connection
    from etl.src.s3_landing import load_fixtures_from_s3, write_fixtures_to_s3
    ds = ds or dlq.now_utc().date().isoformat()
    summary = {"due": 0, "dead_rechecked": 0, "resolved": 0, "still_bad": 0, "skipped": 0}
    for key in [k for k in dlq.list_keys(PREFIX) if k.endswith("/open.json")]:
        entry = dlq.read_json(key)
        if entry is None:
            summary["skipped"] += 1
            continue
        if dlq.is_due(entry):
            summary["due"] += 1
        elif dlq.dead_recheck_due(entry):
            summary["dead_rechecked"] += 1
        else:
            summary["skipped"] += 1
            continue
        lid, season = entry["api_league_id"], entry["season"]
        try:
            fixtures = extract_fixtures_field(
                fetch_fixtures(FIXTURES_ENDPOINT, params={"league": lid, "season": season}))
        except Exception as exc:
            _failed_attempt(key, entry, f"fetch failed: {type(exc).__name__}: {exc}")
            summary["still_bad"] += 1
            continue
        if not fixtures:
            _failed_attempt(key, entry, "empty fixtures response")
            summary["still_bad"] += 1
            continue
        landing = write_fixtures_to_s3(fixtures, lid, season, ds)
        conn = get_db_connection()
        try:
            _, bad = load_fixtures_from_s3(landing, conn, source="replay", detailed=True)
            conn.commit()
            dlq.run_after_commit(conn)
        except Exception:
            conn.rollback()
            dlq.discard_after_commit(conn)
            raise
        finally:
            conn.close()
        summary["still_bad" if bad else "resolved"] += 1
    logger.info("fixture row DLQ replay: %s", summary)
    return summary
