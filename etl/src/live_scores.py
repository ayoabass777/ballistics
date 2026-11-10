"""
Pure logic for the live poller (docs/live_scores.md): parse API-Football's
live fixtures, diff them against what is stored, and decide when to poll.

No database or network here; etl/src/live_poller.py does the IO.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Provider short statuses (https://www.api-football.com/documentation-v3#tag/Fixtures).
IN_PLAY = frozenset({"1H", "HT", "2H", "ET", "BT", "P", "SUSP", "INT", "LIVE"})
FINAL = frozenset({"FT", "AET", "PEN"})
# Statuses after which the match will not continue today.
STOPPED = frozenset({"PST", "CANC", "ABD", "AWD", "WO"})
DONE = FINAL | STOPPED

# A tracked fixture is in the live window from shortly before kickoff until
# well after a normal finish (extra time, penalties, a late kickoff).
BEFORE_KICKOFF = timedelta(minutes=10)
AFTER_KICKOFF = timedelta(minutes=180)
# With nothing in the window, check again at most this often: kickoffs move.
IDLE_RECHECK = timedelta(minutes=10)

STATE_FIELDS = ("status", "elapsed", "extra", "home_goals", "away_goals",
                "home_red_cards", "away_red_cards")
RED_CARDS = frozenset({"red card", "second yellow card"})


def _event_minute(event: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    time = event.get("time") or {}
    return time.get("elapsed"), time.get("extra")


def _side(team_id: Any, home_id: Any, away_id: Any) -> Optional[str]:
    if team_id is None:
        return None
    if team_id == home_id:
        return "home"
    if team_id == away_id:
        return "away"
    return None


def parse_events(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Provider events → rows with a stable event_key.

    The provider has no event id. The key uses the event's own fields plus an
    occurrence number, so two identical events (rare: the same player booked
    twice in one minute) stay distinct and a re-poll maps to the same keys.
    """
    teams = item.get("teams") or {}
    home_id = (teams.get("home") or {}).get("id")
    away_id = (teams.get("away") or {}).get("id")
    seen: Counter = Counter()
    rows = []
    for event in item.get("events") or []:
        minute, extra = _event_minute(event)
        side = _side((event.get("team") or {}).get("id"), home_id, away_id)
        player = event.get("player") or {}
        assist = event.get("assist") or {}
        etype = (event.get("type") or "").strip() or "Unknown"
        detail = (event.get("detail") or "").strip() or None
        base = "|".join(str(x) for x in (etype, detail, side, player.get("id") or player.get("name"),
                                          minute, extra))
        seen[base] += 1
        rows.append({
            "event_key": f"{base}|{seen[base]}",
            "minute": minute, "extra": extra, "side": side, "type": etype, "detail": detail,
            "player": player.get("name"), "assist": assist.get("name"),
        })
    return rows


def red_cards(events: Iterable[Dict[str, Any]]) -> Tuple[int, int]:
    count = Counter(e["side"] for e in events
                    if e["type"] == "Card" and (e["detail"] or "").lower() in RED_CARDS)
    return count["home"], count["away"]


def parse_fixture(item: Dict[str, Any], fixture_id: int) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """One provider fixture → (state row, event rows)."""
    fixture = item.get("fixture") or {}
    status = fixture.get("status") or {}
    goals = item.get("goals") or {}
    events = parse_events(item)
    home_red, away_red = red_cards(events)
    state = {
        "fixture_id": fixture_id,
        "api_fixture_id": fixture.get("id"),
        "status": status.get("short") or "LIVE",
        "elapsed": status.get("elapsed"),
        "extra": status.get("extra"),
        "home_goals": goals.get("home"),
        "away_goals": goals.get("away"),
        "home_red_cards": home_red,
        "away_red_cards": away_red,
    }
    return state, events


def state_changed(old: Optional[Dict[str, Any]], new: Dict[str, Any]) -> bool:
    return old is None or any(old.get(k) != new.get(k) for k in STATE_FIELDS)


# The provider often sends an event, then corrects it a poll or two later: the
# scorer filled in, the minute moved by one, the booked player swapped,
# substitutions renumbered. A correction updates the stored event in place;
# an event with no plausible correction is revoked.
#
# Revoked only means the provider stopped sending the event. It is not a VAR
# signal: corrections this matching misses (a minute moved by 15, a goal
# turned into an own goal) and phantom entries are revoked too. VAR decisions
# arrive as the provider's own events (type 'Var': Goal cancelled, Penalty
# confirmed, Card upgrade confirmed, ...); read those for VAR.
CORRECTION_MINUTES = 3


def _same_player(a: Optional[str], b: Optional[str]) -> bool:
    return not a or not b or a.strip().lower() == b.strip().lower()


def _correction_rank(old: Dict[str, Any], new: Dict[str, Any]) -> Optional[Tuple[int, int, int]]:
    """How well `new` explains a missing `old` event; None if it cannot."""
    if (old.get("type") or "").lower() != (new.get("type") or "").lower() or old.get("side") != new.get("side"):
        return None
    a, b = old.get("minute"), new.get("minute")
    gap = abs(a - b) if a is not None and b is not None else 0
    if gap > CORRECTION_MINUTES:
        return None
    same_detail = old.get("detail") == new.get("detail")
    player = _same_player(old.get("player"), new.get("player"))
    # A different player only when nothing else moved (a booking reassigned).
    if not player and not (gap == 0 and same_detail):
        return None
    return (0 if player else 1, 0 if same_detail else 1, gap)


def diff_events(stored: Dict[str, Dict[str, Any]], current: List[Dict[str, Any]]
                ) -> Tuple[List[Dict[str, Any]], List[str], List[Tuple[str, Dict[str, Any]]]]:
    """stored: event_key → stored row (with `revoked`). Returns (rows to
    upsert, keys to revoke, corrections as (old key, corrected row)).

    Upserted rows are new events and revoked ones that came back (the
    provider sometimes drops an event for one poll). An active event missing
    from this poll is matched to a new event that corrects it; if none fits it
    is revoked.
    """
    keys = {e["event_key"] for e in current}
    fresh = [e for e in current if e["event_key"] not in stored]
    missing = [k for k, row in stored.items() if not row.get("revoked") and k not in keys]
    corrections: List[Tuple[str, Dict[str, Any]]] = []
    revoke: List[str] = []
    for key in missing:
        ranked = [(r, i) for i, e in enumerate(fresh)
                  if (r := _correction_rank(stored[key], e)) is not None]
        if not ranked:
            revoke.append(key)
            continue
        corrections.append((key, fresh.pop(min(ranked)[1])))
    corrected = {e["event_key"] for _, e in corrections}
    upsert = [e for e in current if e["event_key"] not in corrected
              and stored.get(e["event_key"], {"revoked": True}).get("revoked")]
    return upsert, revoke, corrections


def window_bounds(now: datetime) -> Tuple[datetime, datetime]:
    """Kickoffs in [now - AFTER_KICKOFF, now + BEFORE_KICKOFF] may be live."""
    return now - AFTER_KICKOFF, now + BEFORE_KICKOFF


def idle_sleep(now: datetime, next_kickoff: Optional[datetime]) -> float:
    """Seconds to sleep when no tracked fixture is in the live window."""
    wake = now + IDLE_RECHECK
    if next_kickoff is not None:
        wake = min(wake, next_kickoff - BEFORE_KICKOFF)
    return max(30.0, (wake - now).total_seconds())


def provider_errors(payload: Dict[str, Any]) -> Optional[str]:
    """API-Football returns HTTP 200 with an `errors` object for plan, quota
    and token problems; an empty list or dict means none."""
    errors = payload.get("errors")
    if not errors:
        return None
    if isinstance(errors, dict):
        return "; ".join(f"{k}: {v}" for k, v in errors.items())
    return "; ".join(str(e) for e in errors)


def should_be_live(window: List[Dict[str, Any]], now: datetime, minimum: int = 3) -> int:
    """How many tracked fixtures are clearly under way (kicked off 15–90
    minutes ago) and not known to be finished or stopped. An empty live
    response while several are under way is a provider or plan problem, not
    a quiet afternoon (the July 2026 plan lapse looked like one)."""
    lo, hi = now - timedelta(minutes=90), now - timedelta(minutes=15)
    count = sum(1 for f in window if lo <= f["kickoff_utc"] <= hi and f.get("status") not in DONE)
    return count if count >= minimum else 0
