"""Data decorators for API payloads: data-quality annotations (D148).

Decorator pattern on data. A page payload is built by a component; each
decorator wraps a component, builds through it, and adds one kind of
data-quality annotation to the result without the builder knowing about it.
Decorators stack, so a page gets exactly the checks it needs, and a new check
is a new decorator rather than an edit to every builder.

Every decorated payload carries one block the frontend can render the same
way on any page:

    "data_quality": {
        "ok": false,
        "issues": [
            {"code": "standings_incomplete", "section": "context",
             "severity": "warning", "since": "2026-10-17T00:10:28Z",
             "message": "League table may be out of date: Club Leon couldn't be updated.",
             "detail": {"teams": ["Club Leon"], "rows": 1}}
        ]
    }

`section` says where to show it (context, fixture, strength / patterns /
goals via "engine"). A decorator that fails adds nothing: a data-quality
check never breaks the page.

    MATCH_PAGE = decorate(Component(build_match_core),
                          StandingsQuality, FixtureRowsQuality, Freshness)
    MATCH_PAGE.build(cur, fixture_id=…)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable

log = logging.getLogger(__name__)

STANDINGS_STALE_HOURS = 48
ENGINE_STALE_DAYS = 2


class Component:
    """The undecorated builder: (cur, **kw) -> payload dict | None."""

    def __init__(self, build: Callable):
        self._build = build

    def build(self, cur, **kw):
        payload = self._build(cur, **kw)
        if payload is not None:
            payload.setdefault("data_quality", {"ok": True, "issues": []})
        return payload


class DataDecorator:
    """Wraps a component; subclasses add issues in `annotate`."""

    def __init__(self, inner):
        self.inner = inner

    def build(self, cur, **kw):
        payload = self.inner.build(cur, **kw)
        if payload is None:
            return None
        try:
            for issue in self.annotate(payload, cur, **kw) or []:
                add_issue(payload, **issue)
        except Exception as exc:          # never break the page
            log.warning("%s skipped: %s", type(self).__name__, exc)
        return payload

    def annotate(self, payload: dict, cur, **kw) -> list[dict]:
        raise NotImplementedError


def add_issue(payload: dict, *, code: str, section: str, message: str,
              severity: str = "warning", since=None, detail: dict | None = None) -> None:
    block = payload.setdefault("data_quality", {"ok": True, "issues": []})
    if any(i["code"] == code and i["section"] == section for i in block["issues"]):
        return
    block["issues"].append({"code": code, "section": section, "severity": severity,
                            "since": _iso(since), "message": message, "detail": detail or {}})
    block["ok"] = False


def decorate(component, *decorators):
    """decorate(Component(f), A, B) == B(A(Component(f)))."""
    for d in decorators:
        component = d(component)
    return component


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


def _parse(v):
    if v is None or isinstance(v, datetime):
        return v
    dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _context(payload: dict) -> dict:
    """Context lives at the top level of /context and under `context` on /match."""
    return payload.get("context") if "context" in payload else payload


def _names(rows, n=3):
    names = [str(r) for r in rows if r][:n]
    return ", ".join(names)


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------

class StandingsQuality(DataDecorator):
    """League table has rows the API sent that we couldn't load (D147)."""

    def annotate(self, payload, cur, **kw):
        table = (_context(payload) or {}).get("table") or {}
        if not table.get("incomplete"):
            return []
        stale = [r["team_name"] for g in table.get("groups") or [] for r in g.get("rows") or []
                 if r.get("stale")]
        who = f"{_names(stale)} couldn't be updated" if stale else "some rows couldn't be loaded"
        return [{"code": "standings_incomplete", "section": "context",
                 "message": f"League table may be out of date: {who}.",
                 "detail": {"teams": stale, "rows": table.get("unprocessed_rows") or len(stale)}}]


FIXTURE_ROWS_SQL = """
SELECT s.status, s.unprocessed_rows, s.unprocessed_since
FROM raw.fact_fixtures f
JOIN dim.dim_league_seasons ls ON ls.league_season_id = f.league_season_id
     AND f.fixture_id = %(fixture_id)s
JOIN dim.dim_leagues l ON l.league_id = ls.league_id
JOIN raw.raw_fixtures_status s ON s.api_league_id = l.api_league_id AND s.season = ls.season
WHERE s.status <> 'ok'
"""


class FixtureRowsQuality(DataDecorator):
    """The league season has fixtures parked in the row DLQ (D148): H2H, form
    and records may be missing a match."""

    def annotate(self, payload, cur, fixture_id=None, **kw):
        if fixture_id is None:
            return []
        cur.execute("SELECT to_regclass('raw.raw_fixtures_status') IS NOT NULL AS ready")
        ready = cur.fetchone()
        if not ready or not (ready.get("ready") if isinstance(ready, dict) else ready[0]):
            return []
        cur.execute(FIXTURE_ROWS_SQL, {"fixture_id": fixture_id})
        row = cur.fetchone()
        if not row or not isinstance(row, dict) or "status" not in row:
            return []
        n = row.get("unprocessed_rows") or 0
        return [{"code": "fixtures_incomplete", "section": "fixture",
                 "since": row.get("unprocessed_since"),
                 "message": (f"{n} fixture{'s' if n != 1 else ''} in this league couldn't be loaded; "
                             "head-to-head and form may be missing a match."),
                 "detail": {"rows": n, "status": row.get("status")}}]


class Freshness(DataDecorator):
    """Snapshots older than expected: standings > 48 h, engine run > 2 days."""

    def __init__(self, inner, now: Callable[[], datetime] | None = None):
        super().__init__(inner)
        self.now = now or (lambda: datetime.now(timezone.utc))

    def annotate(self, payload, cur, **kw):
        issues = []
        fixture = payload.get("fixture") or {}
        now = self.now()
        standings_at = _parse(fixture.get("standings_updated_at") or payload.get("standings_updated_at"))
        if standings_at and (now - standings_at).total_seconds() > STANDINGS_STALE_HOURS * 3600:
            issues.append({"code": "standings_stale", "section": "context", "since": standings_at,
                           "message": f"League table last updated {standings_at:%d %b}."})
        run_ds = fixture.get("run_ds")
        # After kickoff the page deliberately keeps the last pre-kickoff
        # forecast (D152); its age is not staleness.
        kickoff = _parse(fixture.get("kickoff_utc"))
        kept_pre_kickoff = (fixture.get("forecast_from_latest_run") is False
                            and kickoff is not None and kickoff <= now)
        if run_ds and not kept_pre_kickoff:
            run_day = datetime.fromisoformat(str(run_ds)).date()
            if (now.date() - run_day).days > ENGINE_STALE_DAYS:
                issues.append({"code": "engine_stale", "section": "engine", "since": run_ds,
                               "message": f"Strength, goals and patterns are from {run_day:%d %b}."})
        return issues
