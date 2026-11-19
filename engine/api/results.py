"""Played games for a past day on Matches, in the live-score list format.

GET /api/results?zone=Europe/London&day=2026-10-26

One local day of finished fixtures from raw.fact_fixtures. With no day, the
latest day up to today that has results. previous_day / next_day point at the
nearest days with results, so the page never steps onto an empty day.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from engine.api.crests import crest_url
from engine.api.live import leading_pick

# The local date of a kickoff in the member's zone.
LOCAL_DAY = "(f.kickoff_utc AT TIME ZONE %(zone)s)::date"

LATEST_SQL = f"""
SELECT max({LOCAL_DAY}) AS day FROM raw.fact_fixtures f
WHERE f.is_played AND {LOCAL_DAY} <= %(today)s
"""

NEIGHBOURS_SQL = f"""
SELECT (SELECT max({LOCAL_DAY}) FROM raw.fact_fixtures f WHERE f.is_played AND {LOCAL_DAY} < %(day)s) AS previous_day,
       (SELECT min({LOCAL_DAY}) FROM raw.fact_fixtures f WHERE f.is_played AND {LOCAL_DAY} > %(day)s
                                                             AND {LOCAL_DAY} <= %(today)s) AS next_day
"""

DAY_SQL = f"""
SELECT f.fixture_id, f.fixture_status, f.kickoff_utc,
       f.home_team_name, f.away_team_name, f.league_id, f.league_name, f.country_name,
       f.home_team_fulltime_goal, f.away_team_fulltime_goal,
       f.home_team_halftime_goal, f.away_team_halftime_goal,
       f.home_logo_file_id, f.away_logo_file_id, l.logo_file_id AS league_logo_file_id,
       a.discovery
FROM raw.fact_fixtures f
LEFT JOIN dim.dim_leagues l ON l.league_id = f.league_id
-- Kicked-off fixtures carry their pre-kickoff forecast (D152): the last
-- complete run that analysed them, as on the live list. Upcoming ones get
-- theirs from Discover, so their (large) payloads are not read here.
LEFT JOIN LATERAL (
    SELECT m.payload->'discovery' AS discovery
    FROM raw.match_analysis m
    JOIN raw.engine_runs r ON r.run_ds = m.run_ds AND r.status = 'complete'
    WHERE m.fixture_id = f.fixture_id
    ORDER BY m.run_ds DESC LIMIT 1) a ON f.kickoff_utc <= now()
WHERE f.is_played AND {LOCAL_DAY} = %(day)s
ORDER BY f.league_tier NULLS LAST, f.country_name, f.league_name, f.kickoff_utc, f.fixture_id
"""


def _zone(zone: str) -> ZoneInfo:
    try:
        return ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError("Unknown timezone") from exc


def _day(value: Optional[date]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def card(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fixture_id": row["fixture_id"],
        "status": row["fixture_status"],
        "home": row["home_team_name"], "away": row["away_team_name"],
        "home_goals": row["home_team_fulltime_goal"], "away_goals": row["away_team_fulltime_goal"],
        "home_ht": row["home_team_halftime_goal"], "away_ht": row["away_team_halftime_goal"],
        "league_id": row["league_id"], "league": row["league_name"], "country": row["country_name"],
        "kickoff": row["kickoff_utc"].isoformat(),
        "home_crest_url": crest_url("teams", row.get("home_logo_file_id")),
        "away_crest_url": crest_url("teams", row.get("away_logo_file_id")),
        "league_logo_url": crest_url("leagues", row.get("league_logo_file_id")),
        # The match's leading pre-kickoff finding, so a played game can show
        # whether it landed (frontend lib/live.ts trackPick).
        "pick": leading_pick(row.get("discovery")),
    }


def results(cur, zone: str = "UTC", day: Optional[date] = None, now: Optional[datetime] = None) -> Dict[str, Any]:
    today = (now or datetime.now(timezone.utc)).astimezone(_zone(zone)).date()
    if day is not None and day > today:
        raise ValueError("Results are only available up to today")
    params = {"zone": zone, "today": today}
    if day is None:
        cur.execute(LATEST_SQL, params)
        found = cur.fetchone()
        day = found["day"] if found else None
        if day is None:
            return {"day": None, "zone": zone, "previous_day": None, "next_day": None, "matches": []}
    params["day"] = day
    cur.execute(NEIGHBOURS_SQL, params)
    near = cur.fetchone() or {}
    cur.execute(DAY_SQL, params)
    return {"day": day.isoformat(), "zone": zone,
            "previous_day": _day(near.get("previous_day")), "next_day": _day(near.get("next_day")),
            "matches": [card(r) for r in cur.fetchall()]}


def fixtures(cur, zone: str, date_from: date, date_to: date) -> Dict[str, Any]:
    _zone(zone)
    if date_to < date_from:
        raise ValueError("End date must be on or after start date")
    query = DAY_SQL.replace(
        "f.is_played AND " + LOCAL_DAY + " = %(day)s",
        LOCAL_DAY + " BETWEEN %(date_from)s AND %(date_to)s",
    )
    cur.execute(query, {"zone": zone, "date_from": date_from, "date_to": date_to})
    return {"day": None, "zone": zone, "previous_day": None, "next_day": None,
            "matches": [card(row) for row in cur.fetchall()]}
