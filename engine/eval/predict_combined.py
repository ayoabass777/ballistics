"""
Combined predictor — joins Poisson GLM, Mismatch Elo, and Bayesian Streak
predictions per fixture.

Runs all three models, joins on fixture_id, and outputs a unified view where
each fixture shows all qualifying markets from every engine side by side.

Usage:
    python -m engine.eval.predict_combined
    python -m engine.eval.predict_combined --days 14 --min-gap 100
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import json

import yaml

from engine.eval.predict_upcoming import (
    predict_upcoming,
    CALIBRATED_THRESHOLD,
    _tier,
)
from engine.eval.predict_mismatch import (
    predict_mismatch,
    _market_label,
    MismatchPick,
)
from engine.eval.bayesian_streaks import get_fixture_beliefs


def run_combined(days: int = 7, min_gap: int = 100) -> pd.DataFrame:
    print("=" * 60)
    print("  POISSON GLM MODEL")
    print("=" * 60)
    poisson_picks = predict_upcoming(days=days)

    print()
    print("=" * 60)
    print("  MISMATCH ELO MODEL")
    print("=" * 60)
    mismatch_picks, fixture_strength = predict_mismatch(days=days, min_gap=min_gap)

    # Build per-fixture Poisson summary
    poisson_by_fixture = {}
    if not poisson_picks.empty:
        for _, row in poisson_picks.iterrows():
            fid = int(row["fixture_id"])
            if fid not in poisson_by_fixture:
                poisson_by_fixture[fid] = {
                    "fixture_id": fid,
                    "kickoff": row["kickoff"],
                    "home": row["home"],
                    "away": row["away"],
                    "league": row["league"],
                    "home_elo": row.get("home_elo"),
                    "away_elo": row.get("away_elo"),
                    "league_goal_avg": row.get("league_goal_avg"),
                    "poisson_picks": [],
                }
            poisson_by_fixture[fid]["poisson_picks"].append({
                "line": row["line"],
                "direction": row["direction"],
                "confidence": row["probability"],
                "raw_confidence": row.get("raw_probability"),
                "tier": _tier(row["probability"]),
            })

    # Build per-fixture mismatch summary
    mismatch_by_fixture = {}
    for p in mismatch_picks:
        fid = p.fixture_id
        if fid not in mismatch_by_fixture:
            mismatch_by_fixture[fid] = {
                "fixture_id": fid,
                "kickoff": p.kickoff,
                "home": p.home_team,
                "away": p.away_team,
                "league": p.league,
                "favourite": p.favourite,
                "underdog": p.underdog,
                "fav_elo": p.fav_elo,
                "dog_elo": p.dog_elo,
                "gap": p.gap,
                "gap_bin": p.gap_bin,
                "h2h_wins": p.h2h_wins,
                "h2h_draws": p.h2h_draws,
                "h2h_losses": p.h2h_losses,
                "mismatch_picks": [],
            }
        mismatch_by_fixture[fid]["mismatch_picks"].append({
            "market": p.market,
            "label": _market_label(p.market, p.favourite),
            "confidence": p.confidence,
            "sample_size": p.sample_size,
            "h2h_boost": p.h2h_boost,
        })

    # Bayesian streak beliefs
    print()
    print("=" * 60)
    print("  BAYESIAN STREAK MODEL")
    print("=" * 60)
    try:
        from engine.eval.predict_mismatch import _get_conn
        bayes_conn = _get_conn()
        bayesian_by_fixture = get_fixture_beliefs(bayes_conn, days=days)
        bayes_conn.close()
        print(f"  {len(bayesian_by_fixture)} fixtures with Bayesian beliefs")
    except Exception as e:
        print(f"  Bayesian model skipped: {e}")
        bayesian_by_fixture = {}

    # Merge on fixture_id
    all_fixture_ids = (
        set(poisson_by_fixture.keys())
        | set(mismatch_by_fixture.keys())
        | set(bayesian_by_fixture.keys())
    )

    combined = []
    for fid in all_fixture_ids:
        poisson = poisson_by_fixture.get(fid)
        mismatch = mismatch_by_fixture.get(fid)
        bayes = bayesian_by_fixture.get(fid)

        base = poisson or mismatch
        if base is None and bayes:
            base = {"kickoff": None, "home": "", "away": "", "league": ""}
        entry = {
            "fixture_id": fid,
            "kickoff": base["kickoff"],
            "home": base["home"],
            "away": base["away"],
            "league": base["league"],
            "poisson_picks": poisson["poisson_picks"] if poisson else [],
            "mismatch_picks": mismatch["mismatch_picks"] if mismatch else [],
            "bayesian_beliefs": bayes if bayes else {"home": [], "away": []},
            "both_models": poisson is not None and mismatch is not None,
        }

        if mismatch:
            entry["favourite"] = mismatch["favourite"]
            entry["gap"] = mismatch["gap"]
            entry["gap_bin"] = mismatch["gap_bin"]
            entry["h2h"] = f"{mismatch['h2h_wins']}W-{mismatch['h2h_draws']}D-{mismatch['h2h_losses']}L"

        if poisson:
            entry["league_goal_avg"] = poisson.get("league_goal_avg")

        if fid in fixture_strength:
            entry["team_strength"] = fixture_strength[fid]

        combined.append(entry)

    combined.sort(key=lambda x: (not x["both_models"], pd.Timestamp(x["kickoff"])))
    return combined


def _count_models(c: dict) -> int:
    n = 0
    if c["poisson_picks"]:
        n += 1
    if c["mismatch_picks"]:
        n += 1
    bayes = c.get("bayesian_beliefs", {})
    if bayes.get("home") or bayes.get("away"):
        n += 1
    return n


def print_combined(combined: list[dict]) -> None:
    if not combined:
        print("No picks from any model.")
        return

    all_three = [c for c in combined if _count_models(c) == 3]
    two_models = [c for c in combined if _count_models(c) == 2]
    one_model = [c for c in combined if _count_models(c) == 1]

    print(f"\n{'='*120}")
    print(f"  COMBINED PREDICTIONS")
    print(f"  Poisson GLM + Mismatch Elo + Bayesian Streaks (joined on fixture_id)")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Fixtures: {len(all_three)} all 3 models | {len(two_models)} 2 models | {len(one_model)} 1 model")
    print(f"{'='*120}")

    if all_three:
        print(f"\n{'─'*120}")
        print(f"  ALL 3 MODELS  ({len(all_three)} fixtures)")
        print(f"{'─'*120}")
        for c in all_three:
            _print_fixture(c)

    if two_models:
        print(f"\n{'─'*120}")
        print(f"  2 MODELS  ({len(two_models)} fixtures)")
        print(f"{'─'*120}")
        for c in two_models:
            _print_fixture(c)

    if one_model:
        print(f"\n{'─'*120}")
        print(f"  1 MODEL  ({len(one_model)} fixtures)")
        print(f"{'─'*120}")
        for c in one_model:
            _print_fixture(c)


def _print_fixture(c: dict) -> None:
    dt = pd.Timestamp(c["kickoff"]).strftime("%a %b %d %H:%M")
    match = f"{c['home']} vs {c['away']}"
    gap_str = f"  gap={c['gap']:.0f} ({c['gap_bin']})" if "gap" in c else ""
    h2h_str = f"  H2H: {c['h2h']}" if c.get("h2h") and c["h2h"] != "0W-0D-0L" else ""
    lg_avg = f"  lg_avg={c['league_goal_avg']}" if c.get("league_goal_avg") else ""

    print(f"\n  {dt}  {match}  [{c['league']}]{gap_str}{h2h_str}{lg_avg}")

    # Team strength ranks
    ts = c.get("team_strength")
    if ts:
        for side_key, label in (("home_team", c["home"]), ("away_team", c["away"])):
            profile = ts.get(side_key)
            if profile and profile.get("overall"):
                ov = profile["overall"]
                ar = ov.get("attack_rank")
                dr = ov.get("defense_rank")
                ls = ov.get("league_size")
                if ar and dr and ls:
                    print(f"    Strength:  {label:<20s}  atk {ar}/{ls}  def {dr}/{ls}")

    if c["poisson_picks"]:
        for p in c["poisson_picks"]:
            tier = p["tier"]
            print(f"    Poisson:   {p['direction']} {p['line']}  →  {p['confidence']:.0%} (Tier {tier})")

    if c["mismatch_picks"]:
        for m in c["mismatch_picks"]:
            boost = f" {m['h2h_boost']}" if m.get("h2h_boost") else ""
            print(f"    Mismatch:  {m['label']:<28s}  →  {m['confidence']:.0%} (n={m['sample_size']}){boost}")

    bayes = c.get("bayesian_beliefs", {})
    for side in ("home", "away"):
        for b in bayes.get(side, []):
            if b.get("primary") is False:
                continue  # secondary all-context view; keep terminal output clean
            trend = {"hot": "↑", "cold": "↓", "stable": ""}[b["trend"]]
            streak_str = f" [{b['streak']}]" if b["streak"] >= 2 else ""
            delta = b["posterior"] - b["base_rate"]
            sign = "+" if delta >= 0 else ""
            ctx = str(b.get("context", "all")).upper()
            kn = f"k={b['k']}/n={b['n']}" if b.get("k") is not None else f"n={b['n']}"
            boost_str = f" ⚡+{b['boost']:.0%}" if b.get("boost") else ""
            print(
                f"    Bayesian:  {b['team']:<20s} {b['event']:<16s} [{ctx:<4s}]  →  "
                f"P={b['posterior']:.0%} (base {b['base_rate']:.0%}, {sign}{delta:.0%}) "
                f"{kn}{streak_str}{boost_str} {trend}"
            )


def _build_output_data(combined: list[dict]) -> dict:
    data = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "models": ["poisson_glm", "mismatch_elo_gap", "bayesian_streaks"],
        "fixtures": [],
    }

    for c in combined:
        entry = {
            "fixture_id": c["fixture_id"],
            "fixture": f"{c['home']} vs {c['away']}",
            "kickoff": pd.Timestamp(c["kickoff"]).strftime("%Y-%m-%d %H:%M UTC"),
            "league": c["league"],
            "both_models": c["both_models"],
        }
        if "gap" in c:
            entry["elo_gap"] = c["gap"]
            entry["gap_bin"] = c["gap_bin"]
        if c.get("h2h") and c["h2h"] != "0W-0D-0L":
            entry["h2h"] = c["h2h"]
        if c.get("favourite"):
            entry["favourite"] = c["favourite"]
        if c.get("league_goal_avg"):
            entry["league_goal_avg"] = c["league_goal_avg"]

        if c.get("team_strength"):
            entry["team_strength"] = c["team_strength"]

        picks = []
        for p in c["poisson_picks"]:
            picks.append({
                "model": "poisson",
                "market": f"{p['direction']} {p['line']}",
                "confidence": p["confidence"],
                "tier": p["tier"],
            })
        for m in c["mismatch_picks"]:
            pick_entry = {
                "model": "mismatch",
                "market": m["label"],
                "confidence": m["confidence"],
                "sample_size": m["sample_size"],
            }
            if m.get("h2h_boost"):
                pick_entry["h2h_boost"] = m["h2h_boost"]
            picks.append(pick_entry)

        bayes = c.get("bayesian_beliefs", {})
        for side in ("home", "away"):
            for b in bayes.get(side, []):
                picks.append({
                    "model": "bayesian",
                    "market": f"{b['team']} {b['event']}",
                    "confidence": b["posterior"],
                    "base_rate": b["base_rate"],
                    "n_matches": b["n"],
                    "k_matches": b.get("k"),
                    "context": b.get("context"),
                    "primary": b.get("primary", True),
                    "streak_boost": b.get("boost"),
                    "streak": b["streak"],
                    "trend": b["trend"],
                    "side": side,
                })

        entry["picks"] = picks
        data["fixtures"].append(entry)

    return data


def save_combined(combined: list[dict]) -> tuple[str, str]:
    ds = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data = _build_output_data(combined)

    json_path = Path(__file__).parent / f"combined_picks_{ds}.json"
    yaml_path = Path(__file__).parent / f"combined_picks_{ds}.yaml"

    with open(json_path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    with open(yaml_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"\nCombined picks saved to {json_path} and {yaml_path}")
    return str(json_path), str(yaml_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combined Poisson + Mismatch predictions")
    parser.add_argument("--days", type=int, default=7, help="Days ahead")
    parser.add_argument("--min-gap", type=int, default=100, help="Minimum Elo gap for mismatch")
    args = parser.parse_args()

    combined = run_combined(days=args.days, min_gap=args.min_gap)
    print_combined(combined)
    if combined:
        save_combined(combined)
