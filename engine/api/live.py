"""Live scores for the research API (docs/live_scores.md, phase 1).

GET /api/live                 today's live and just-finished tracked fixtures
GET /api/live/{fixture_id}    one fixture's live state and events

Read-only over raw.live_fixture_state and raw.live_events, which only the
live poller writes. The pre-kickoff forecast is never changed here (D152);
the page compares it with the live score itself.
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from engine.api.crests import crest_url
from engine.api.discover import KINDS, PICK_FIELDS

# Finished fixtures stay in the list this long, then the nightly update and
# the Forecast record take over.
FINISHED_FOR = "6 hours"
# The poller writes every 15 s; many members polling the list share one read.
LIST_TTL = 5.0

FIELDS = """s.fixture_id, s.status, s.elapsed, s.extra, s.home_goals, s.away_goals,
       s.home_red_cards, s.away_red_cards, s.changed_at, s.polled_at, s.finished_at"""

LIST_SQL = f"""
SELECT {FIELDS},
       f.home_team_name, f.away_team_name, f.league_id, f.league_name, f.country_name,
       f.kickoff_utc, f.home_logo_file_id, f.away_logo_file_id,
       l.logo_file_id AS league_logo_file_id, a.discovery
FROM raw.live_fixture_state s
JOIN raw.fact_fixtures f ON f.fixture_id = s.fixture_id
LEFT JOIN dim.dim_leagues l ON l.league_id = f.league_id
-- The fixture's last complete run, which is its pre-kickoff forecast (D152).
LEFT JOIN LATERAL (
    SELECT m.payload->'discovery' AS discovery
    FROM raw.match_analysis m
    JOIN raw.engine_runs r ON r.run_ds = m.run_ds AND r.status = 'complete'
    WHERE m.fixture_id = s.fixture_id
    ORDER BY m.run_ds DESC LIMIT 1) a ON true
WHERE s.finished_at IS NULL AND s.polled_at > now() - interval '{FINISHED_FOR}'
   OR s.finished_at > now() - interval '{FINISHED_FOR}'
ORDER BY f.kickoff_utc, s.fixture_id
"""

ONE_SQL = f"SELECT {FIELDS} FROM raw.live_fixture_state s WHERE s.fixture_id = %(fixture_id)s"

EVENTS_SQL = """
SELECT minute, extra, side, type, detail, player, assist
FROM raw.live_events
WHERE fixture_id = %(fixture_id)s AND revoked_at IS NULL AND type <> 'subst'
ORDER BY minute NULLS LAST, extra NULLS FIRST, event_id
"""

_list_cache: Dict[str, Any] = {"at": -LIST_TTL, "body": None}


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def state(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fixture_id": row["fixture_id"],
        "status": row["status"],
        "minute": row["elapsed"],
        "extra": row["extra"],
        "home_goals": row["home_goals"],
        "away_goals": row["away_goals"],
        "home_red_cards": row["home_red_cards"],
        "away_red_cards": row["away_red_cards"],
        "finished": row["finished_at"] is not None,
        "changed_at": _iso(row["changed_at"]),
        "polled_at": _iso(row["polled_at"]),
    }


def leading_pick(discovery: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The match's strongest pre-kickoff finding, chosen as Matches chooses its
    preview (discover.assemble); private scores never leave the API."""
    sections = (discovery or {}).get("sections") or {}
    candidates = [sections[k] for k in KINDS
                  if sections.get(k) and isinstance(sections[k].get("score"), (int, float))]
    best = max(candidates, key=lambda p: p["score"], default=None)
    return {k: best.get(k) for k in PICK_FIELDS} if best else None


def card(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **state(row),
        "pick": leading_pick(row.get("discovery")),
        "home": row["home_team_name"], "away": row["away_team_name"],
        "league_id": row["league_id"], "league": row["league_name"], "country": row["country_name"],
        "kickoff": _iso(row["kickoff_utc"]),
        "home_crest_url": crest_url("teams", row.get("home_logo_file_id")),
        "away_crest_url": crest_url("teams", row.get("away_logo_file_id")),
        "league_logo_url": crest_url("leagues", row.get("league_logo_file_id")),
    }


def live_list(cur, clock=time.monotonic) -> Dict[str, Any]:
    now = clock()
    if _list_cache["body"] is not None and now - _list_cache["at"] < LIST_TTL:
        return _list_cache["body"]
    cur.execute(LIST_SQL)
    body = {"matches": [card(r) for r in cur.fetchall()]}
    _list_cache.update(at=now, body=body)
    return body


def live_fixture(cur, fixture_id: int) -> Dict[str, Any]:
    cur.execute(ONE_SQL, {"fixture_id": fixture_id})
    row = cur.fetchone()
    if row is None:
        return {"state": None, "events": []}
    cur.execute(EVENTS_SQL, {"fixture_id": fixture_id})
    events: List[Dict[str, Any]] = [dict(e) for e in cur.fetchall()]
    return {"state": state(row), "events": events}
