"""
Live poller: API-Football live fixtures → raw.live_fixture_state and
raw.live_events (docs/live_scores.md, phase 1).

A long-running service, not an Airflow task: it polls `fixtures?live=all`
every LIVE_POLL_SECONDS (15, the provider's own refresh) while a tracked
fixture is in its live window, and sleeps otherwise, so it spends no calls
overnight. It writes only changes and sends NOTIFY live_changed with the
fixture_id for each.

It never writes raw.raw_fixtures: the nightly pipeline owns results.

Run: python -m etl.src.live_poller
"""
from __future__ import annotations

import os
import signal
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import logging
import requests

from etl.src import live_scores as ls
from etl.src.config import LOGS_PATH
from etl.src.logger import get_logger

logger = get_logger(__name__, log_path=os.path.join(LOGS_PATH, "live_poller_log.txt"))
# A service, so also log to stdout for `docker compose logs live-poller`.
if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
           for h in logger.handlers):
    _stdout = logging.StreamHandler()
    _stdout.setLevel(logging.INFO)
    _stdout.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(_stdout)

POLL_SECONDS = float(os.getenv("LIVE_POLL_SECONDS", "15"))
# Calls left today below which the poller slows to protect the nightly pipeline.
DAILY_RESERVE = int(os.getenv("LIVE_DAILY_RESERVE", "1500"))
SLOW_POLL_SECONDS = 60.0
# Calls left below which the poller stops until the quota resets (00:00 UTC).
DAILY_FLOOR = int(os.getenv("LIVE_DAILY_FLOOR", "300"))
FAILURES_BEFORE_ALERT = 5
ALERT_EVERY_SECONDS = 3600
IDS_PER_CALL = 20

WINDOW_SQL = """
    SELECT r.fixture_id, r.api_fixture_id, r.kickoff_utc,
           coalesce(s.status, r.fixture_status) AS status
    FROM raw.raw_fixtures r
    LEFT JOIN raw.live_fixture_state s ON s.fixture_id = r.fixture_id
    WHERE r.kickoff_utc BETWEEN %(lo)s AND %(hi)s
"""
NEXT_KICKOFF_SQL = """
    SELECT min(kickoff_utc) FROM raw.raw_fixtures
    WHERE kickoff_utc > %(hi)s AND fixture_status = 'NS'
"""
IDS_SQL = "SELECT api_fixture_id, fixture_id FROM raw.raw_fixtures WHERE api_fixture_id = ANY(%(ids)s)"
STATES_SQL = f"""
    SELECT fixture_id, api_fixture_id, {', '.join(ls.STATE_FIELDS)}
    FROM raw.live_fixture_state WHERE fixture_id = ANY(%(ids)s)
"""
# Fixtures we have seen live that have not reached a final status.
OPEN_SQL = f"""
    SELECT fixture_id, api_fixture_id FROM raw.live_fixture_state
    WHERE status <> ALL(%(done)s) AND polled_at > now() - interval '6 hours'
"""
EVENTS_SQL = """
    SELECT fixture_id, event_key, revoked_at IS NOT NULL AS revoked,
           type, detail, side, player, minute
    FROM raw.live_events WHERE fixture_id = ANY(%(ids)s)
"""
UPSERT_STATE_SQL = """
    INSERT INTO raw.live_fixture_state
      (fixture_id, api_fixture_id, status, elapsed, extra, home_goals, away_goals,
       home_red_cards, away_red_cards, polled_at, changed_at, finished_at)
    VALUES (%(fixture_id)s, %(api_fixture_id)s, %(status)s, %(elapsed)s, %(extra)s,
            %(home_goals)s, %(away_goals)s, %(home_red_cards)s, %(away_red_cards)s,
            %(now)s, %(now)s, CASE WHEN %(final)s THEN %(now)s END)
    ON CONFLICT (fixture_id) DO UPDATE SET
      status = EXCLUDED.status, elapsed = EXCLUDED.elapsed, extra = EXCLUDED.extra,
      home_goals = EXCLUDED.home_goals, away_goals = EXCLUDED.away_goals,
      home_red_cards = EXCLUDED.home_red_cards, away_red_cards = EXCLUDED.away_red_cards,
      polled_at = EXCLUDED.polled_at,
      changed_at = CASE WHEN %(changed)s THEN EXCLUDED.changed_at ELSE raw.live_fixture_state.changed_at END,
      finished_at = coalesce(raw.live_fixture_state.finished_at, EXCLUDED.finished_at)
"""
UPSERT_EVENT_SQL = """
    INSERT INTO raw.live_events
      (fixture_id, event_key, minute, extra, side, type, detail, player, assist, seen_at)
    VALUES (%(fixture_id)s, %(event_key)s, %(minute)s, %(extra)s, %(side)s, %(type)s,
            %(detail)s, %(player)s, %(assist)s, %(now)s)
    ON CONFLICT (fixture_id, event_key) DO UPDATE SET revoked_at = NULL
"""
# The provider corrected an event: same row, so it keeps event_id and seen_at.
CORRECT_EVENT_SQL = """
    UPDATE raw.live_events
    SET event_key = %(event_key)s, minute = %(minute)s, extra = %(extra)s, side = %(side)s,
        type = %(type)s, detail = %(detail)s, player = %(player)s, assist = %(assist)s,
        revoked_at = NULL
    WHERE fixture_id = %(fixture_id)s AND event_key = %(old_key)s
"""
REVOKE_SQL = """
    UPDATE raw.live_events SET revoked_at = %(now)s
    WHERE fixture_id = %(fixture_id)s AND event_key = ANY(%(keys)s) AND revoked_at IS NULL
"""


class ProviderError(RuntimeError):
    pass


class Provider:
    """API-Football over HTTP. Tracks the daily calls left from its headers."""

    def __init__(self, url: str, key: str, session: Optional[requests.Session] = None):
        self.url = url.rstrip("/") + "/fixtures"
        self.session = session or requests.Session()
        self.session.headers["x-apisports-key"] = key
        self.remaining: Optional[int] = None

    def get(self, params: Dict[str, str]) -> List[Dict[str, Any]]:
        resp = self.session.get(self.url, params=params, timeout=20)
        left = resp.headers.get("x-ratelimit-requests-remaining")
        if left is not None and left.lstrip("-").isdigit():
            self.remaining = int(left)
        resp.raise_for_status()
        payload = resp.json()
        problem = ls.provider_errors(payload)
        if problem:
            raise ProviderError(problem)
        return payload.get("response") or []

    def live(self) -> List[Dict[str, Any]]:
        return self.get({"live": "all"})

    def by_ids(self, api_ids: List[int]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for i in range(0, len(api_ids), IDS_PER_CALL):
            out += self.get({"ids": "-".join(str(x) for x in api_ids[i:i + IDS_PER_CALL])})
        return out


class Store:
    """raw.live_* reads and writes. Each write_poll is one transaction."""

    def __init__(self, connect: Callable[[], Any]):
        self.connect = connect
        self.conn = None

    def _cur(self):
        if self.conn is None or self.conn.closed:
            self.conn = self.connect()
        return self.conn.cursor()

    def _rows(self, sql: str, params: Dict[str, Any]) -> List[tuple]:
        try:
            with self._cur() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            self.conn.commit()
            return rows
        except Exception:
            self._reset()
            raise

    def _reset(self):
        try:
            if self.conn is not None:
                self.conn.rollback()
        except Exception:
            self.conn = None

    def window(self, now: datetime) -> List[Dict[str, Any]]:
        lo, hi = ls.window_bounds(now)
        return [{"fixture_id": r[0], "api_fixture_id": r[1], "kickoff_utc": r[2], "status": r[3]}
                for r in self._rows(WINDOW_SQL, {"lo": lo, "hi": hi})]

    def next_kickoff(self, now: datetime) -> Optional[datetime]:
        _, hi = ls.window_bounds(now)
        rows = self._rows(NEXT_KICKOFF_SQL, {"hi": hi})
        return rows[0][0] if rows else None

    def fixture_ids(self, api_ids: List[int]) -> Dict[int, int]:
        return dict(self._rows(IDS_SQL, {"ids": api_ids})) if api_ids else {}

    def open_fixtures(self) -> Dict[int, int]:
        """fixture_id → api_fixture_id for fixtures seen live and not done."""
        return {r[0]: r[1] for r in self._rows(OPEN_SQL, {"done": sorted(ls.DONE)})}

    def write_poll(self, parsed: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]],
                   now: datetime) -> List[int]:
        """Upsert states and events; returns the fixture_ids that changed."""
        if not parsed:
            return []
        ids = [state["fixture_id"] for state, _ in parsed]
        changed: List[int] = []
        try:
            with self._cur() as cur:
                cur.execute(STATES_SQL, {"ids": ids})
                cols = [d[0] for d in cur.description]
                old = {r[0]: dict(zip(cols, r)) for r in cur.fetchall()}
                cur.execute(EVENTS_SQL, {"ids": ids})
                stored: Dict[int, Dict[str, Dict[str, Any]]] = {}
                for fixture_id, key, revoked, etype, detail, side, player, minute in cur.fetchall():
                    stored.setdefault(fixture_id, {})[key] = {
                        "revoked": revoked, "type": etype, "detail": detail, "side": side,
                        "player": player, "minute": minute}
                for state, events in parsed:
                    fid = state["fixture_id"]
                    upsert, revoke, corrections = ls.diff_events(stored.get(fid, {}), events)
                    moved = (ls.state_changed(old.get(fid), state)
                             or bool(upsert) or bool(revoke) or bool(corrections))
                    cur.execute(UPSERT_STATE_SQL, {**state, "now": now, "changed": moved,
                                                   "final": state["status"] in ls.DONE})
                    for old_key, event in corrections:
                        cur.execute(CORRECT_EVENT_SQL, {**event, "fixture_id": fid, "old_key": old_key})
                    for event in upsert:
                        cur.execute(UPSERT_EVENT_SQL, {**event, "fixture_id": fid, "now": now})
                    if revoke:
                        cur.execute(REVOKE_SQL, {"fixture_id": fid, "keys": revoke, "now": now})
                    if moved:
                        changed.append(fid)
                        cur.execute("SELECT pg_notify('live_changed', %s)", (str(fid),))
            self.conn.commit()
        except Exception:
            self._reset()
            raise
        return changed


class Poller:
    def __init__(self, provider: Provider, store: Store,
                 alert: Callable[[str, str], Any],
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        self.provider, self.store, self.alert, self.clock = provider, store, alert, clock
        self.failures = 0
        self.last_alert: Dict[str, float] = {}

    def _alert_once(self, kind: str, subject: str, body: str) -> None:
        """At most one alert of each kind an hour."""
        now = time.monotonic()
        if now - self.last_alert.get(kind, -ALERT_EVERY_SECONDS) < ALERT_EVERY_SECONDS:
            return
        self.last_alert[kind] = now
        logger.error("%s: %s", subject, body)
        try:
            self.alert(subject, body)
        except Exception as exc:  # alerting must never stop the poller
            logger.error("Alert delivery failed (%s)", type(exc).__name__)

    def cycle(self) -> float:
        """One poll. Returns seconds to sleep before the next."""
        now = self.clock()
        window = [f for f in self.store.window(now) if f["status"] not in ls.DONE]
        open_live = self.store.open_fixtures()
        if not window and not open_live:
            return ls.idle_sleep(now, self.store.next_kickoff(now))
        try:
            items = self.provider.live()
            ids = self.store.fixture_ids([i["fixture"]["id"] for i in items if (i.get("fixture") or {}).get("id")])
            parsed = [ls.parse_fixture(i, ids[i["fixture"]["id"]]) for i in items
                      if (i.get("fixture") or {}).get("id") in ids]
            # Fixtures that dropped out of the live list have usually finished:
            # fetch them by id once for the final score and events.
            seen = {state["fixture_id"] for state, _ in parsed}
            gone = {fid: api for fid, api in open_live.items() if fid not in seen}
            if gone:
                by_api = {api: fid for fid, api in gone.items()}
                for item in self.provider.by_ids(sorted(by_api)):
                    api = (item.get("fixture") or {}).get("id")
                    if api in by_api:
                        parsed.append(ls.parse_fixture(item, by_api[api]))
            changed = self.store.write_poll(parsed, now)
        except (requests.RequestException, ProviderError, ValueError, KeyError) as exc:
            self.failures += 1
            logger.warning("Live poll failed (%s, %d in a row): %s",
                           type(exc).__name__, self.failures, exc)
            if self.failures >= FAILURES_BEFORE_ALERT:
                self._alert_once("provider", "✗ Ballistics live scores: provider failing",
                                 f"{self.failures} live polls in a row failed. Last error: "
                                 f"{type(exc).__name__}: {exc}. Live scores are stale.")
            return min(300.0, POLL_SECONDS * 2 ** min(self.failures, 4))
        self.failures = 0
        expected = ls.should_be_live(window, now)
        if not parsed and expected:
            self._alert_once("empty", "✗ Ballistics live scores: empty live feed",
                             f"The provider returned no live tracked fixtures while {expected} "
                             "tracked fixtures kicked off 15–90 minutes ago. Check the "
                             "API-Football plan and quota (an empty response looked like a "
                             "quiet weekend during the July 2026 plan lapse).")
        if changed:
            logger.info("Live poll: %d tracked live, %d changed", len(parsed), len(changed))
        return self._pace(now)

    def _pace(self, now: datetime) -> float:
        left = self.provider.remaining
        if left is not None and left < DAILY_FLOOR:
            self._alert_once("quota", "✗ Ballistics live scores paused: API quota low",
                             f"{left} API-Football calls left today; live polling paused "
                             "until the quota resets at 00:00 UTC to keep calls for the "
                             "nightly pipeline.")
            midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            return max(60.0, (midnight.timestamp() + 86400 - now.timestamp()) + 60)
        if left is not None and left < DAILY_RESERVE:
            return SLOW_POLL_SECONDS
        return POLL_SECONDS


def main() -> None:
    from etl.src.config import API
    from etl.src.extract_metadata import get_db_connection
    from etl.src.data_detector.notify import send_alert

    if not API.get("key") or not API.get("url"):
        raise SystemExit("API_KEY and API_URL are required")
    poller = Poller(Provider(API["url"], API["key"]), Store(get_db_connection), send_alert)
    stopping = {"now": False}

    def stop(*_):
        stopping["now"] = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logger.info("Live poller started (every %.0fs while fixtures are live)", POLL_SECONDS)
    while not stopping["now"]:
        try:
            delay = poller.cycle()
        except Exception as exc:  # database down: keep the service up and retry
            logger.error("Live poller cycle failed (%s): %s", type(exc).__name__, exc)
            delay = 60.0
        end = time.monotonic() + delay
        while not stopping["now"] and time.monotonic() < end:
            time.sleep(min(1.0, end - time.monotonic()))
    logger.info("Live poller stopped")


if __name__ == "__main__":
    main()
