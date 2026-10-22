"""
Standings with unprocessable rows: DLQ on S3, hourly replay, and the flag that
marks a league's table as incomplete (D147).

A standings row the API sends without a team id (Club León, Liga MX, 15 and
17 Oct 2026) cannot be keyed, so it is parked instead of loaded:

  nightly refresh   good rows load, the bad rows go to
                    s3://<bucket>/dlq/standings/{api_league_id}/{season}/open.json,
                    pruning is off for that league (the team's last good row
                    stays), and raw.raw_league_standings_status flags the league
  hourly replay     re-pulls just that league; clean → load with pruning,
                    entry moved to dlq/resolved/..., flag cleared; still bad →
                    attempts + 1, next try in an hour; after 24 → dead + alert
  next nightly run  a clean response resolves the entry too, even a dead one

The flag flows to dbt (mart_league_standings.standings_incomplete,
is_stale_row) and the match page API (table.incomplete, row.stale).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from etl.src import dlq
from etl.src.data_detector.monitoring import monitored
from etl.src.logger import get_logger

logger = get_logger(__name__)

PREFIX = "dlq/standings/"
STATUS_TABLE = "raw.raw_league_standings_status"

STATUS_DDL = f"""
CREATE TABLE IF NOT EXISTS {STATUS_TABLE} (
    league_season_id   INT PRIMARY KEY,
    api_league_id      INT NOT NULL,
    season             INT NOT NULL,
    status             TEXT NOT NULL,          -- ok | unprocessed | dead
    unprocessed_rows   INT NOT NULL DEFAULT 0,
    unprocessed_teams  TEXT[],
    reason             TEXT,                   -- e.g. null_team_id
    unprocessed_since  TIMESTAMPTZ,
    replay_attempts    INT NOT NULL DEFAULT 0,
    dlq_key            TEXT,                   -- NULL while unprocessed = S3 write failed
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def dlq_key(api_league_id: int, season: int) -> str:
    return f"{PREFIX}{api_league_id}/{season}/open.json"


# ---------------------------------------------------------------------------
# Validation + parking (used by the @dead_letter decorator)
# ---------------------------------------------------------------------------

def standings_row_problem(row: Dict[str, Any]) -> Optional[str]:
    """Why a standings row can't be loaded, or None when it can."""
    if row.get(dlq.DLQ_REASON):
        return str(row[dlq.DLQ_REASON])
    for field in ("api_team_id", "rank", "points", "games_played", "wins",
                  "draws", "losses", "goals_for", "goals_against", "goal_diff", "group_order"):
        value = row.get(field)
        if value is not None:
            try:
                if isinstance(value, bool) or str(int(value)) != str(value).strip():
                    return f"invalid_integer:{field}"
                if not -(2**31) <= int(value) < 2**31:
                    return f"integer_out_of_range:{field}"
            except (ValueError, TypeError, OverflowError):
                return f"invalid_integer:{field}"
    if row.get("api_team_id") is None:
        return "null_team_id"          # part of the primary key
    return None


def park_standings_rows(bad: List[Dict[str, Any]], *, league: Dict[str, Any], ds: str,
                        source: str = "nightly", **_: Any) -> Optional[Tuple[str, Dict[str, Any], bool]]:
    """Write a league's bad rows to its open DLQ entry. A replay that still
    finds them counts as a failed attempt; the nightly run doesn't (it only
    refreshes the rows). Returns (key, entry, created), or None when S3 is unreachable:
    the league is still flagged in the DB, with no dlq_key, and the daily
    check reports that as an error."""
    key = dlq_key(league["api_league_id"], league["season"])
    reasons = sorted({r[dlq.DLQ_REASON] for r in bad})
    try:
        existing = dlq.read_json(key)
        created = existing is None
        entry = existing or {
            "kind": "standings", **{k: league[k] for k in ("league_season_id", "api_league_id", "season")},
            **dlq.new_state(), "ds_seen": [],
        }
        entry["rows"] = bad
        entry["reasons"] = reasons
        entry["teams"] = [str(r.get("team_name")) for r in bad]
        entry["last_seen"] = dlq._iso(dlq.now_utc())
        entry["ds_seen"] = sorted(set(entry.get("ds_seen") or []) | {str(ds)})
        became_dead = False
        if source == "replay" and not created:   # a new entry starts at 0 attempts
            entry, became_dead = dlq.record_failed_attempt(entry, f"still unprocessable: {', '.join(reasons)}")
        dlq.write_json(key, entry)
    except Exception as exc:          # S3 down must not fail the standings task
        logger.error("standings DLQ write failed for %s: %s", key, exc, exc_info=True)
        return None
    logger.warning("league_season_id=%s: parked %d standings row(s) (%s) at s3://%s/%s",
                   league["league_season_id"], len(bad), ", ".join(entry["teams"]),
                   dlq.default_bucket(), key)
    if became_dead:
        dlq.alert_dead("standings", key, entry,
                       f"Standings for league {league['api_league_id']} season {league['season']} "
                       f"({', '.join(entry['teams'])})")
    return key, entry, created


def resolve_open_entry(league: Dict[str, Any], how: str) -> Optional[str]:
    """Clean refresh: move the league's open entry (if any) to dlq/resolved/."""
    key = dlq_key(league["api_league_id"], league["season"])
    try:
        entry = dlq.read_json(key)
        if entry is None:
            return None
        dest = dlq.resolve_entry(key, entry, how)
    except Exception as exc:
        logger.error("standings DLQ resolve failed for %s: %s", key, exc, exc_info=True)
        return None
    logger.info("league_season_id=%s: standings DLQ entry resolved by %s → %s",
                league["league_season_id"], how, dest)
    return dest


# ---------------------------------------------------------------------------
# The flag
# ---------------------------------------------------------------------------

def set_status(league: Dict[str, Any], bad: List[Dict[str, Any]],
               parked: Optional[tuple], conn=None) -> None:
    """Flag (or clear) the league in raw.raw_league_standings_status."""
    from etl.src.extract_metadata import get_db_connection
    entry = parked[1] if parked else {}
    if not bad:
        status = "ok"
    else:
        status = "dead" if entry.get("status") == dlq.DEAD else "unprocessed"
    own = conn is None
    conn = conn or get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(STATUS_DDL)
            cur.execute(
                f"""
                INSERT INTO {STATUS_TABLE} AS s
                    (league_season_id, api_league_id, season, status, unprocessed_rows,
                     unprocessed_teams, reason, unprocessed_since, replay_attempts, dlq_key, updated_at)
                VALUES (%(lsid)s, %(lid)s, %(season)s, %(status)s, %(n)s, %(teams)s, %(reason)s,
                        CASE WHEN %(status)s = 'ok' THEN NULL ELSE now() END,
                        %(attempts)s, %(key)s, now())
                ON CONFLICT (league_season_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    unprocessed_rows = EXCLUDED.unprocessed_rows,
                    unprocessed_teams = EXCLUDED.unprocessed_teams,
                    reason = EXCLUDED.reason,
                    unprocessed_since = CASE WHEN EXCLUDED.status = 'ok' THEN NULL
                                             ELSE COALESCE(s.unprocessed_since, EXCLUDED.unprocessed_since) END,
                    replay_attempts = EXCLUDED.replay_attempts,
                    dlq_key = EXCLUDED.dlq_key,
                    updated_at = now()
                """,
                {"lsid": league["league_season_id"], "lid": league["api_league_id"],
                 "season": league["season"], "status": status, "n": len(bad),
                 "teams": [str(r.get("team_name")) for r in bad] or None,
                 "reason": ", ".join(sorted({r[dlq.DLQ_REASON] for r in bad})) or None,
                 "attempts": int(entry.get("attempts") or 0),
                 "key": parked[0] if parked else None},
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if own:
            conn.close()


# ---------------------------------------------------------------------------
# Loading one league
# ---------------------------------------------------------------------------

@monitored(dataset="standings", target_table="raw.raw_league_standings")
@dlq.dead_letter(validate=standings_row_problem, park=park_standings_rows)
def load_league_standings(rows: List[Dict[str, Any]], *, league: Dict[str, Any], ds: str,
                          source: str = "nightly", quarantined=(), parked=None) -> dlq.LoadResult:
    """Upsert a league's good rows. With rows quarantined, pruning stays off
    (the parked team's last good row is kept) and the league is flagged;
    a clean load prunes, clears the flag and resolves any open DLQ entry."""
    from etl.src.update_standings import upsert_standings
    inserted = upsert_standings(rows, prune=not quarantined)
    resolved = None if quarantined else resolve_open_entry(league, how=source)
    try:
        set_status(league, list(quarantined), parked)
    except Exception as exc:        # the flag must not stop the other leagues
        logger.error("standings status update failed for %s: %s", league, exc)
    return dlq.LoadResult(loaded=inserted, parked=list(quarantined),
                          dlq_key=parked[0] if parked else None,
                          new_entry=bool(parked and parked[2]), resolved=bool(resolved),
                          dlq_write_failed=bool(quarantined) and not parked)


def refresh_league(league: Dict[str, Any], ds: str, source: str = "nightly",
                   payload: Optional[list] = None) -> dlq.LoadResult:
    """Fetch (unless given), flatten and load one league season.
    An empty response loads nothing and changes nothing."""
    from etl.src.update_standings import extract_standings, fetch_standings
    if payload is None:
        payload = fetch_standings(league["api_league_id"], league["season"])
    rows = extract_standings(payload)
    if not rows:
        return dlq.LoadResult(loaded=0)
    for row in rows:
        row["league_season_id"] = league["league_season_id"]
        row["api_league_id"] = league["api_league_id"]
        row["season"] = league["season"]
    return load_league_standings(rows, league=league, ds=ds, source=source)


# ---------------------------------------------------------------------------
# Hourly replay
# ---------------------------------------------------------------------------

def _failed_attempt(key: str, entry: Dict[str, Any], error: str) -> None:
    try:
        entry, became_dead = dlq.record_failed_attempt(entry, error)
        dlq.write_json(key, entry)
        league = {k: entry[k] for k in ("league_season_id", "api_league_id", "season")}
        set_status(league, entry.get("rows") or [{"team_name": t, dlq.DLQ_REASON: "unknown"}
                                                 for t in entry.get("teams") or []], (key, entry))
        if became_dead:
            dlq.alert_dead("standings", key, entry,
                           f"Standings for league {entry['api_league_id']} season {entry['season']}")
    except Exception:
        # Keep the open entry for a later run if storage/status recording is
        # unavailable; one entry must not prevent the remaining retries.
        logger.exception("Could not record DLQ replay failure for %s: %s", key, error)


def replay_standings_dlq(ds: Optional[str] = None) -> Dict[str, int]:
    """Retry every due standings entry: one API call per league season."""
    from etl.src.update_standings import extract_standings, fetch_standings
    ds = ds or dlq.now_utc().date().isoformat()
    summary = {"due": 0, "resolved": 0, "still_bad": 0, "skipped": 0}
    for key in [k for k in dlq.list_keys(PREFIX) if k.endswith("/open.json")]:
        entry = dlq.read_json(key)
        if entry is None or not dlq.is_due(entry):
            summary["skipped"] += 1
            continue
        summary["due"] += 1
        league = {k: entry[k] for k in ("league_season_id", "api_league_id", "season")}
        try:
            payload = fetch_standings(league["api_league_id"], league["season"])
        except Exception as exc:
            _failed_attempt(key, entry, f"fetch failed: {type(exc).__name__}: {exc}")
            summary["still_bad"] += 1
            continue
        try:
            if not extract_standings(payload):
                raise ValueError("empty standings response")
            _, bad = refresh_league(league, ds, source="replay", payload=payload)
        except Exception as exc:
            _failed_attempt(key, entry, f"load failed: {type(exc).__name__}: {exc}")
            summary["still_bad"] += 1
            continue
        summary["still_bad" if bad else "resolved"] += 1
    logger.info("standings DLQ replay: %s", summary)
    return summary
