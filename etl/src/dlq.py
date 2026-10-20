"""
Dead letter queue (DLQ): shared retry policy, S3 storage and the @dead_letter
decorator for row-level data errors.

Error handling (D147):
  network error   timeout, DNS, 5xx, 429 → retry the task (Airflow / HTTP
                  backoff). Only when every retry fails does the unit (a
                  league season) go to the DLQ.
  data error      a bad row in a good response (e.g. standings row with no
                  team id) → never fail the task: park the row in the DLQ,
                  load the rest, re-pull hourly.
  our side        auth, DB constraint not covered by validation, code bug →
                  fail loudly; replaying would not fix it. Never DLQ'd.

Retry policy for every DLQ entry (fixtures and standings):
  replayed hourly (replay_dag), next_retry_at = last attempt + 1 h;
  after MAX_ATTEMPTS failed replays (24 = one day) the entry is `dead`:
  no more hourly retries, an alert goes out, and the daily data check reports
  it as an error until it is resolved. Fixture-row entries, which no nightly
  run reloads, still get one recheck a day (dead_recheck_due) so a fix on the
  API side clears them.

Entry state (inside the entry's JSON on S3):
  status          open | dead | resolved
  attempts        failed replays so far
  first_seen      when the entry was first written
  last_attempt_at last replay
  next_retry_at   when the hourly replay may try again
  last_error      why the last attempt failed
"""
from __future__ import annotations

import functools
import inspect
import json
import weakref
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

RETRY_INTERVAL = timedelta(hours=1)
MAX_ATTEMPTS = 24
DEAD_RECHECK_INTERVAL = timedelta(days=1)

OPEN, DEAD, RESOLVED = "open", "dead", "resolved"
DLQ_REASON = "_dlq_reason"          # key the decorator adds to each parked row


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Retry state
# ---------------------------------------------------------------------------

def new_state(now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or now_utc()
    return {"status": OPEN, "attempts": 0, "first_seen": _iso(now),
            "last_attempt_at": None, "next_retry_at": _iso(now + RETRY_INTERVAL),
            "last_error": None}


def with_state(entry: Dict[str, Any], now: Optional[datetime] = None) -> Dict[str, Any]:
    """Entries written before D147 have no state: treat them as open and due."""
    if "status" in entry:
        return entry
    now = now or now_utc()
    first = entry.get("timestamp") or _iso(now)
    return {**entry, "status": OPEN, "attempts": 0, "first_seen": first,
            "last_attempt_at": None, "next_retry_at": _iso(now), "last_error": entry.get("error")}


def is_due(entry: Dict[str, Any], now: Optional[datetime] = None) -> bool:
    entry = with_state(entry, now)
    if entry["status"] != OPEN:
        return False
    nxt = _parse(entry.get("next_retry_at"))
    return nxt is None or nxt <= (now or now_utc())


def dead_recheck_due(entry: Dict[str, Any], now: Optional[datetime] = None) -> bool:
    """A dead entry whose last attempt is a day old: worth one more look.
    A failed recheck counts an attempt but stays dead and doesn't re-alert."""
    entry = with_state(entry, now)
    if entry["status"] != DEAD:
        return False
    last = _parse(entry.get("last_attempt_at"))
    return last is None or last + DEAD_RECHECK_INTERVAL <= (now or now_utc())


def record_failed_attempt(entry: Dict[str, Any], error: str,
                          now: Optional[datetime] = None) -> Tuple[Dict[str, Any], bool]:
    """One more failed replay. Returns (entry, became_dead)."""
    now = now or now_utc()
    entry = dict(with_state(entry, now))
    entry["attempts"] = int(entry.get("attempts") or 0) + 1
    entry["last_attempt_at"] = _iso(now)
    entry["last_error"] = str(error)[:500]
    became_dead = entry["attempts"] >= MAX_ATTEMPTS and entry["status"] == OPEN
    if became_dead:
        entry["status"] = DEAD
        entry["next_retry_at"] = None
    elif entry["status"] == OPEN:
        entry["next_retry_at"] = _iso(now + RETRY_INTERVAL)
    return entry, became_dead


def mark_resolved(entry: Dict[str, Any], how: str, now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or now_utc()
    return {**with_state(entry, now), "status": RESOLVED, "resolved_at": _iso(now),
            "resolved_by": how, "next_retry_at": None}


def age_hours(entry: Dict[str, Any], now: Optional[datetime] = None) -> float:
    first = _parse(with_state(entry, now).get("first_seen"))
    if first is None:
        return 0.0
    return max(0.0, ((now or now_utc()) - first).total_seconds() / 3600)


# ---------------------------------------------------------------------------
# S3 storage (JSON)
# ---------------------------------------------------------------------------

def _s3():
    import boto3
    return boto3.client("s3")


def default_bucket() -> str:
    from etl.src.s3_landing import S3_BUCKET
    return S3_BUCKET


def read_json(key: str, bucket: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """The JSON at key, or None when there is no such object."""
    s3 = _s3()
    try:
        obj = s3.get_object(Bucket=bucket or default_bucket(), Key=key)
    except s3.exceptions.NoSuchKey:
        return None
    return json.loads(obj["Body"].read())


def write_json(key: str, payload: Dict[str, Any], bucket: Optional[str] = None) -> str:
    _s3().put_object(Bucket=bucket or default_bucket(), Key=key,
                     Body=json.dumps(payload, default=str, indent=1),
                     ContentType="application/json")
    return key


def delete_key(key: str, bucket: Optional[str] = None) -> None:
    _s3().delete_object(Bucket=bucket or default_bucket(), Key=key)


def list_keys(prefix: str, bucket: Optional[str] = None) -> List[str]:
    paginator = _s3().get_paginator("list_objects_v2")
    keys: List[str] = []
    for page in paginator.paginate(Bucket=bucket or default_bucket(), Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def archive_key(key: str, now: Optional[datetime] = None, group: Optional[str] = None) -> str:
    """dlq/standings/262/2026/open.json → dlq/resolved/standings/262/2026/20261020T0100Z.json
    (group="fixtures": dlq/262/2026/2026-10-15/error.json → dlq/resolved/fixtures/262/2026/2026-10-15/…)"""
    stamp = (now or now_utc()).strftime("%Y%m%dT%H%MZ")
    rest = key[len("dlq/"):] if key.startswith("dlq/") else key
    folder = rest.rsplit("/", 1)[0]
    if group:
        folder = f"{group}/{folder}"
    return f"dlq/resolved/{folder}/{stamp}.json"


def resolve_entry(key: str, entry: Dict[str, Any], how: str, bucket: Optional[str] = None,
                  now: Optional[datetime] = None, group: Optional[str] = None) -> str:
    """Move an entry to dlq/resolved/... (audit trail) and remove the open key."""
    done = mark_resolved(entry, how, now)
    dest = write_json(archive_key(key, now, group), done, bucket)
    delete_key(key, bucket)
    return dest


# ---------------------------------------------------------------------------
# After-commit cleanup: resolve entries only once the load is durable
# ---------------------------------------------------------------------------
# A loader that runs in its caller's transaction must not resolve a DLQ entry
# itself: if the caller's commit then fails, the data is rolled back but the
# entry is already gone. It registers the resolution here instead, and the
# caller runs it after commit (or drops it after rollback). A caller that
# forgets leaves the entry open, and the next clean run resolves it.

_after_commit: "weakref.WeakKeyDictionary[Any, List[Callable[[], Any]]]" = weakref.WeakKeyDictionary()


def after_commit(conn, fn: Callable[[], Any]) -> None:
    """Run fn once the caller commits conn (see run_after_commit)."""
    _after_commit.setdefault(conn, []).append(fn)


def run_after_commit(conn) -> None:
    """Call right after conn.commit(). A failing step is logged, not raised:
    the data is committed, and the entry stays open for the next run."""
    for fn in _after_commit.pop(conn, []):
        try:
            fn()
        except Exception as exc:
            from etl.src.logger import get_logger
            get_logger(__name__).error("DLQ after-commit step failed: %s", exc, exc_info=True)


def discard_after_commit(conn) -> None:
    """Call after conn.rollback(): nothing was loaded, so nothing resolves."""
    _after_commit.pop(conn, None)


# ---------------------------------------------------------------------------
# @dead_letter: route bad rows away from a loader
# ---------------------------------------------------------------------------

@dataclass
class LoadResult:
    """What a DLQ-aware loader did. Unpacks as (loaded, parked) so callers can
    write `inserted, bad = load(...)`; the rest is for @monitored."""
    loaded: int
    parked: List[Dict[str, Any]] = field(default_factory=list)
    dlq_key: Optional[str] = None
    new_entry: bool = False          # first sighting: alert once, not every hour
    resolved: bool = False           # a clean load closed an open entry
    dlq_write_failed: bool = False   # rows parked in memory only (S3 down)

    def __iter__(self):
        return iter((self.loaded, self.parked))


def split_rows(rows: Iterable[Dict[str, Any]],
               validate: Callable[[Dict[str, Any]], Optional[str]]
               ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """(good, bad); each bad row carries why under DLQ_REASON."""
    good, bad = [], []
    for row in rows:
        reason = validate(row)
        if reason:
            bad.append({**row, DLQ_REASON: reason})
        else:
            good.append(row)
    return good, bad


def dead_letter(validate: Callable[[Dict[str, Any]], Optional[str]],
                park: Callable[..., Any]):
    """Decorator for a loader whose first argument is a list of rows.

    Rows that `validate` rejects (it returns a reason string) never reach the
    loader: they go to `park(bad_rows, *args, **kwargs)`, which writes them to
    the DLQ. The loader gets the good rows plus `quarantined=<bad rows>` so it
    can act on them (standings: don't prune, flag the league), and, if it
    declares a `parked` parameter, whatever `park` returned. A data error
    never raises; the task carries on with what it could load.

        @dead_letter(validate=standings_row_problem, park=park_standings_rows)
        def load_league(rows, *, league, ds, quarantined=(), parked=None): ...
    """
    def decorate(loader):
        wants_parked = "parked" in inspect.signature(loader).parameters

        @functools.wraps(loader)
        def wrapper(rows, *args, **kwargs):
            good, bad = split_rows(rows, validate)
            parked = park(bad, *args, **kwargs) if bad else None
            if wants_parked:
                kwargs["parked"] = parked
            return loader(good, *args, quarantined=bad, **kwargs)
        wrapper.validate = validate
        wrapper.park = park
        return wrapper
    return decorate


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

CLEARS_ON_NIGHTLY = "It resolves itself if a later nightly run comes back clean."


def alert_dead(kind: str, key: str, entry: Dict[str, Any], what: str,
               clears: str = CLEARS_ON_NIGHTLY) -> None:
    """Immediate alert when an entry gives up; the daily check repeats it."""
    from etl.src.data_detector.notify import send_alert
    subject = f"✗ Ballistics DLQ: {kind} gave up after {entry.get('attempts')} retries"
    body = "\n".join([
        f"{what} is still failing after {entry.get('attempts')} hourly retries "
        f"(first seen {entry.get('first_seen')}).",
        f"Last error: {entry.get('last_error')}",
        f"Entry: s3://{default_bucket()}/{key}",
        f"No more hourly retries. {clears}",
    ])
    send_alert(subject, body)
