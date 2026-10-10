"""
Results review — grade recent predictions against what actually happened.

Reads the saved daily consensus_slips_*.json pools (the predictions the engine
actually made), joins the now-final scoreline for each fixture, marks every pick
hit/miss, and writes consensus_results_<ds>.json for the dashboard's Results tab.

Usage:
    python -m engine.eval.results_review            # last 5 days of played games
    python -m engine.eval.results_review --days 10
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path

from engine.eval.db import get_conn

HERE = Path(__file__).parent


def _grade(market: str, selection: str, line, hg: int, ag: int,
           home: str, away: str) -> bool | None:
    """True=hit, False=miss, None=cannot grade — from the sportybet market shape."""
    total = hg + ag
    if market == "Goal Bounds" and selection == "1-4":
        return 1 <= total <= 4
    if market == "Over/Under":
        ln = float(line)
        return total > ln if "Over" in selection else total < ln
    if market == "Home Team Over/Under":
        return hg >= (2 if "1.5" in selection else 1)
    if market == "Away Team Over/Under":
        return ag >= (2 if "1.5" in selection else 1)
    if market == "Both Teams to Score":
        return hg >= 1 and ag >= 1
    if market == "1X2":
        team = selection.replace(" Win", "").strip()
        if team == home:
            return hg > ag
        if team == away:
            return ag > hg
        return None
    if market == "Double Chance":
        if selection == "1X":
            return hg >= ag
        if selection == "X2":
            return ag >= hg
        return None
    return None


def build_results(days: int = 5, current_rule_only: bool = True) -> dict:
    # 1. collect predictions from recent daily pools (latest per fixture+selection)
    picks: dict = {}
    for f in sorted(glob.glob(str(HERE / "consensus_slips_*.json"))):
        try:
            data = json.load(open(f))
        except Exception:  # noqa: BLE001
            continue
        # Skip pools that predate the current goal-model gates (no Dixon-Coles) so
        # results reflect the engine as it is now, not legacy rule versions.
        if current_rule_only and "dixon_coles" not in (data.get("models") or []):
            continue
        for p in data.get("pool", []):
            key = (p["fixture_id"], p["sportybet_selection"])
            picks[key] = p          # later files overwrite → keep most recent prediction

    if not picks:
        return {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                "results": [], "summary": {}}

    # 2. final scorelines for those fixtures, played within the window
    fids = sorted({k[0] for k in picks})
    conn = get_conn()
    try:
        import pandas as pd
        fx = pd.read_sql(f"""
            SELECT fixture_id, home_team_name AS home, away_team_name AS away,
                   home_team_fulltime_goal AS hg, away_team_fulltime_goal AS ag,
                   kickoff_utc
            FROM raw.raw_fixtures
            WHERE fixture_status = 'FT'
              AND home_team_fulltime_goal IS NOT NULL
              AND kickoff_utc >= now() - make_interval(days => {int(days)})
              AND fixture_id = ANY(%(ids)s)
        """, conn, params={"ids": fids})
    finally:
        conn.close()

    outcomes = {int(r["fixture_id"]): r for _, r in fx.iterrows()}

    # 3. grade + group by fixture
    fixtures: dict = {}
    for (fid, _sel), p in picks.items():
        o = outcomes.get(fid)
        if o is None:
            continue
        hg, ag = int(o["hg"]), int(o["ag"])
        res = _grade(p["sportybet_market"], p["sportybet_selection"], p.get("line"),
                     hg, ag, o["home"], o["away"])
        if res is None:
            continue
        entry = fixtures.setdefault(fid, {
            "fixture_id": fid, "match": p["match"], "home": o["home"], "away": o["away"],
            "league": p.get("league", ""), "country": p.get("country", ""),
            "kickoff": str(o["kickoff_utc"]), "score": f"{hg}-{ag}", "total": hg + ag,
            "picks": [],
        })
        entry["picks"].append({
            "selection": p["sportybet_selection"], "market": p["sportybet_market"],
            "confidence": p["avg_confidence"], "n_models": p["n_models"],
            "models": p["models"], "odds": p.get("odds"), "ev": p.get("ev"),
            "hit": bool(res),
        })

    results = sorted(fixtures.values(), key=lambda x: x["kickoff"], reverse=True)

    # 4. summary
    all_picks = [pk for fx in results for pk in fx["picks"]]
    n = len(all_picks)
    hits = sum(1 for pk in all_picks if pk["hit"])
    by_tier = {}
    for pk in all_picks:
        t = pk["n_models"]
        d = by_tier.setdefault(t, {"n": 0, "hits": 0})
        d["n"] += 1
        d["hits"] += int(pk["hit"])
    summary = {
        "fixtures": len(results), "picks": n,
        "hit_rate": round(hits / n, 4) if n else None,
        "by_tier": {str(t): {"n": d["n"], "hit_rate": round(d["hits"] / d["n"], 4)}
                    for t, d in sorted(by_tier.items(), reverse=True)},
    }
    return {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "days": days, "summary": summary, "results": results}


def main() -> None:
    ap = argparse.ArgumentParser(description="Grade recent predictions vs actual results")
    ap.add_argument("--days", type=int, default=5)
    args = ap.parse_args()
    out = build_results(days=args.days)
    ds = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = HERE / f"consensus_results_{ds}.json"
    with open(path, "w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False, default=str)
    s = out["summary"]
    print(f"Graded {s.get('picks', 0)} picks across {s.get('fixtures', 0)} played fixtures")
    if s.get("hit_rate") is not None:
        print(f"  overall hit rate: {s['hit_rate']:.1%}")
        for t, d in s["by_tier"].items():
            print(f"  {t}-model: {d['hit_rate']:.1%} (n={d['n']})")
    print(f"  saved → {path.name}")


if __name__ == "__main__":
    main()
