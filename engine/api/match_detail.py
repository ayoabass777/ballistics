"""Full match page payload: GET /api/match/{fixture_id} (match-detail-contract.md).

Two sources, each with its own date on the page (D69):
  * Context (standings position, table, H2H, crests) -- live database snapshot
    via engine.api.match_context, dated by ``standings_updated_at``.
  * Strength, Team patterns, Goals and the Context form rows -- page-ready
    sections the export writes to ``raw.match_analysis`` for each run
    (engine/eval/match_store.py). Only the latest *complete* run in
    ``raw.engine_runs`` is read, dated by ``run_ds`` and ``run_finished_at``.

A fixture that is not in the latest run still returns its Context; the
engine sections are null. The section builders below run at export time.

Nothing here exposes engine internals (D61, D115): no rating gaps, posteriors,
calibration values, shrunk rates or Big dog counts reach the payload.

CLI: FORECAST_API_DSN=... python -m engine.api.match_detail 90876
"""
from __future__ import annotations

import json
import os
import re
import sys
import threading
from pathlib import Path

from engine.api import match_context

EXPORT_GLOB = re.compile(r"^consensus_slips_(\d{4}-\d{2}-\d{2})\.json$")
DEFAULT_EXPORT_DIR = Path(__file__).resolve().parents[1] / "eval"
GOALS_MAX = 3
NO_COMPARISON = {"too_few_matches", "unavailable", None}

_cache: dict = {"key": None, "export": None}
_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Export loading (export time and backfill only; the API reads the database)
# ---------------------------------------------------------------------------

def export_dir() -> Path:
    return Path(os.getenv("ENGINE_EXPORT_DIR") or DEFAULT_EXPORT_DIR)


def latest_export_path(directory: Path | None = None) -> Path | None:
    directory = directory or export_dir()
    if not directory.is_dir():
        return None
    runs = sorted((m.group(1), p) for p in directory.iterdir()
                  if (m := EXPORT_GLOB.match(p.name)))
    return runs[-1][1] if runs else None


def index_export(raw: dict) -> dict:
    """Keep only what the match page needs, keyed for lookup."""
    group = raw.get("elo_gap_group") or {}
    return {
        "run_ds": raw.get("run_ds"),
        "fixtures": {int(f["fixture_id"]): f for f in raw.get("fixture_analysis") or []},
        "big_dog": {int(x["fixture_id"]): x for x in raw.get("elo_gap_shortlist") or []},
        "big_dog_leagues": {(r.get("league"), r.get("country")): r
                            for r in group.get("big_dog_leagues") or []},
        "gap_min": group.get("gap_min", 250),
    }


def load_export(path: Path | None = None) -> dict | None:
    """Latest export, re-read only when the file or its mtime changes."""
    path = path or latest_export_path()
    if path is None:
        return None
    key = (str(path), path.stat().st_mtime)
    with _lock:
        if _cache["key"] != key:
            with open(path, encoding="utf-8") as fh:
                _cache["export"] = index_export(json.load(fh))
            _cache["key"] = key
        return _cache["export"]


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------

def form_rows(strength: dict | None) -> dict | None:
    """Context comparison rows: rates and ranks only (no *_shrunk / *_pct)."""
    if not strength:
        return None
    keep = ("attack_rate", "defense_rate", "attack_rank", "defense_rank", "league_size")
    out = {}
    for team in ("home_team", "away_team"):
        views = strength.get(team) or {}
        out[team] = {view: ({k: views[view].get(k) for k in keep} if views.get(view) else None)
                     for view in ("overall", "home", "away")}
    return out


def strength_section(f: dict, export: dict) -> dict | None:
    """Big dog state without counts or rating gaps (D61; owner 2026-10-12)."""
    mm = f.get("mismatch") or []
    if not mm:
        # The engine writes rating rows only for gaps >= 100, so no rows with
        # team data means the sides are close; no team data at all hides the card.
        return {"state": "close"} if f.get("strength") else None
    shortlisted = export["big_dog"].get(int(f["fixture_id"]))
    if shortlisted:
        return {"state": "big_dog", "favourite": shortlisted.get("favourite"),
                "underdog": shortlisted.get("underdog")}
    gap = mm[0].get("gap") or 0
    if gap <= export["gap_min"]:
        return {"state": "close"}
    league = export["big_dog_leagues"].get((f.get("league"), f.get("country"))) or {}
    reason = league.get("reason") or ""
    if "tier" in reason:
        why = "top_flight_only"
    elif reason.startswith("<"):
        why = "not_enough_matches"
    else:
        why = "unpredictable"
    return {"state": "no_label", "reason": why}


def goals_section(f: dict) -> dict | None:
    rows = [r for r in f.get("dixon_coles") or []
            if not str(r.get("market", "")).endswith(" Win")]       # D80
    if not rows:
        return None                                     # hide Goals card
    rows = sorted(rows, key=lambda r: r.get("probability") or 0, reverse=True)[:GOALS_MAX]
    out = []
    for r in rows:
        comparable = r.get("league_comparison") not in NO_COMPARISON
        out.append({
            "market": r.get("market"),
            "probability": round(float(r["probability"]), 2),
            "league_rate": round(r["league_rate"], 2) if comparable and r.get("league_rate") is not None else None,
            "league_rate_context": r.get("league_rate_context") if comparable else None,
            "league_comparison": r.get("league_comparison"),
        })
    return {"rows": out,
            "nothing_stands_out": all(r["league_comparison"] == "typical" for r in out)}


def patterns_section(f: dict) -> dict | None:
    if f.get("patterns") is not None:
        return f["patterns"]
    rows = f.get("bayesian") or []
    if not rows:
        return None
    # Exports written before the selection moved into the engine (D122-D124).
    from engine.eval.team_patterns import select_patterns
    return select_patterns(rows)


def discover_section(f: dict) -> dict | None:
    """Which Discover list put this fixture there, for entry highlighting only."""
    p = f.get("discover_pick") or {}
    if not p.get("list"):
        return None
    return {"list": p["list"], "team": p.get("team"), "event": p.get("event")}


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------

def engine_sections(f: dict, export: dict) -> dict:
    """Page-ready engine sections for one fixture (written per run)."""
    return {
        "strength": strength_section(f, export),
        "patterns": patterns_section(f),
        "goals": goals_section(f),
        "form": form_rows(f.get("strength")),
        "discover": discover_section(f),
    }


LATEST_SQL = """
SELECT r.run_ds, r.finished_at, m.payload
FROM (SELECT run_ds, finished_at FROM raw.engine_runs
      WHERE status = 'complete' ORDER BY run_ds DESC LIMIT 1) r
LEFT JOIN raw.match_analysis m
  ON m.run_ds = r.run_ds AND m.fixture_id = %(fixture_id)s
"""


def read_engine_sections(cur, fixture_id: int) -> tuple[dict | None, dict | None]:
    """(run info, sections) from the latest complete run; Nones if absent."""
    cur.execute("SELECT to_regclass('raw.engine_runs') IS NOT NULL AS ready")
    if not cur.fetchone()["ready"]:
        return None, None                       # store not created yet
    cur.execute(LATEST_SQL, {"fixture_id": fixture_id})
    row = cur.fetchone()
    if row is None:
        return None, None                       # no complete run yet
    run = {"run_ds": row["run_ds"], "finished_at": row["finished_at"]}
    return run, row["payload"]


def build_match(cur, fixture_id: int) -> dict | None:
    ctx = match_context.build_context(cur, fixture_id)
    if ctx is None:
        return None
    run, sec = read_engine_sections(cur, fixture_id)
    sec = sec or {}
    fixture = dict(ctx["fixture"])
    fixture["run_ds"] = match_context._iso(run["run_ds"]) if run and sec else None
    fixture["run_finished_at"] = match_context._iso(run["finished_at"]) if run and sec else None
    fixture["standings_updated_at"] = ctx["standings_updated_at"]
    context = {k: v for k, v in ctx.items() if k not in ("fixture", "standings_updated_at")}
    context["form"] = sec.get("form")
    return {
        "fixture": fixture,
        "context": context,
        "strength": sec.get("strength"),
        "patterns": sec.get("patterns"),
        "goals": sec.get("goals"),
        "discover": sec.get("discover"),
    }


def main(argv):
    from engine.api.forecast_records import read_cursor
    fixture_id = int(argv[1])
    with read_cursor() as cur:
        payload = build_match(cur, fixture_id)
    if payload is None:
        print(f"fixture {fixture_id} not found", file=sys.stderr)
        return 1
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
