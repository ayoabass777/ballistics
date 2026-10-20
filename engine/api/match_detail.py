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

from engine.api import data_quality, match_context

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
        # D137: Big dog gaps are venue-adjusted; exports before it have no edge.
        "home_edge": (raw.get("thresholds") or {}).get("big_dog_home_edge", 0),
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


SLIGHT_EDGE_MAX_GAP = 175   # D138, mirrors engine.eval.consensus_slips


def _tier_context(f: dict) -> dict | None:
    """D138: league tiers as plain labels, never with numbers of their own."""
    t = f.get("tiers") or {}
    return {"home": t["home"], "away": t["away"]} if t.get("home") and t.get("away") else None


def strength_section(f: dict, export: dict) -> dict | None:
    """Strength state plus tier context (D138). Never counts or rating gaps (D61)."""
    out = _strength_state(f, export)
    if out is not None:
        tiers = _tier_context(f)
        if tiers:
            out["tiers"] = tiers
    return out


def _slight(favourite: str, f: dict, gap: float, export: dict) -> dict:
    """D138: slight favourite split by venue-adjusted gap; a label, no rates."""
    return {"state": "slight_favourite", "favourite": favourite,
            "underdog": f.get("away") if favourite == f.get("home") else f.get("home"),
            "level": "slight_edge" if gap <= SLIGHT_EDGE_MAX_GAP else "favourite"}


def _strength_state(f: dict, export: dict) -> dict | None:
    mm = f.get("mismatch") or []
    if not mm:
        # The engine writes rating rows only for gaps >= 100, so no rows with
        # team data means the sides are close; no team data at all hides the card.
        return {"state": "close"} if f.get("strength") else None
    shortlisted = export["big_dog"].get(int(f["fixture_id"]))
    if shortlisted:
        return {"state": "big_dog", "favourite": shortlisted.get("favourite"),
                "underdog": shortlisted.get("underdog")}
    gap = mm[0].get("gap")
    if gap is None:
        return None
    gap = abs(gap)
    if gap <= 100:
        return {"state": "close"}
    # Favourite = the side named in any rating row (match-level rows name nobody).
    favourite = next((team for m in mm for team in (f.get("home"), f.get("away"))
                      if team and str(m.get("market", "")).startswith(f"{team} ")), None)
    edge = export.get("home_edge", 0)
    if favourite is not None:   # D137: the Big dog line uses the venue-adjusted gap
        gap = gap + edge if favourite == f.get("home") else gap - edge
    if gap <= export["gap_min"]:
        if favourite is None:
            return None
        return _slight(favourite, f, gap, export)

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
FROM (SELECT run_ds, COALESCE(generated_at, finished_at) AS finished_at  -- engine build time
      FROM raw.engine_runs
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


def build_match_core(cur, fixture_id: int) -> dict | None:
    """The match page without data-quality annotations (see build_match)."""
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


# Data decorators (engine/api/data_quality.py, D148): each adds one kind of
# issue to payload["data_quality"] without the builder knowing about it.
MATCH_PAGE = data_quality.decorate(
    data_quality.Component(lambda cur, fixture_id: build_match_core(cur, fixture_id)),
    data_quality.StandingsQuality, data_quality.Freshness, data_quality.FixtureRowsQuality)


def build_match(cur, fixture_id: int) -> dict | None:
    """Full match page payload, with its `data_quality` block."""
    return MATCH_PAGE.build(cur, fixture_id=fixture_id)


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
