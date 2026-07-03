"""
Consensus slip generator — multi-model 10x betting slips.

Runs all three engines (Poisson GLM, Mismatch Elo, Bayesian Streaks),
maps every qualifying pick to a unified SportyBet market, scores each
pick by how many models agree, and assembles two ~10x accumulator slips.

Model-to-market mapping:
    Poisson  OVER 2.5 / UNDER 3.5       →  Match Over 2.5 / Under 3.5
    Mismatch fav_to_score                →  Team Over 0.5 (fav side)
    Mismatch over_1.5 / over_2.5         →  Match Over 1.5 / Over 2.5
    Bayesian Over_1_5 / Over_2_5 / etc.  →  Match Over 1.5 / Over 2.5
    Bayesian score_1goal                 →  Team Over 0.5
    Bayesian concede_1                   →  Opponent Over 0.5
    Bayesian Under_2_5 / Under_3_5       →  Match Under 2.5 / Under 3.5

Consensus tiers:
    3-model  →  weight 3.0   (backbone — all three agree)
    2-model  →  weight 2.0   (strong — two independent signals)
    1-model  →  weight 1.0   (solo — only if P >= solo_threshold)

Usage:
    python -m engine.eval.consensus_slips
    python -m engine.eval.consensus_slips --days 10 --target-odds 10
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from engine.eval.predict_upcoming import predict_upcoming, _tier
from engine.eval.predict_mismatch import predict_mismatch, _market_label
from engine.eval.team_strength import load_team_df, compute_team_strength
from engine.eval.bayesian_streaks import (
    get_fixture_beliefs,
    load_event_history,
    compute_league_base_rates,
    compute_beliefs,
    BETTING_EVENTS,
    PRIOR_STRENGTH,
    HALF_LIFE,
    MIN_POSTERIOR_INTERESTING,
)

SOLO_THRESHOLD = 0.80
DUO_THRESHOLD = 0.72
TRIO_THRESHOLD = 0.70

TARGET_ODDS = 5.0
MAX_LEGS = 5
MIN_LEGS = 3
MIN_EST_ODDS = 1.15

IMPLIED_ODDS_MAP = {
    (0.90, 1.00): 1.08,
    (0.85, 0.90): 1.12,
    (0.80, 0.85): 1.18,
    (0.75, 0.80): 1.25,
    (0.70, 0.75): 1.35,
    (0.65, 0.70): 1.48,
}


MARKET_ODDS_BOOST = {
    "Win": 0.30,
    "Double Chance": 0.15,
    "Over 2.5": 0.20,
    "Over 1.5": 0.08,
    "Under 2.5": 0.15,
    "BTTS": 0.20,
}


def _est_odds(prob: float, market: str = "") -> float:
    boost = 0.0
    for pattern, b in MARKET_ODDS_BOOST.items():
        if pattern in market:
            boost = b
            break
    base = None
    for (lo, hi), odds in IMPLIED_ODDS_MAP.items():
        if lo <= prob < hi:
            base = odds
            break
    if base is None:
        if prob >= 1.0:
            base = 1.02
        elif prob < 0.65:
            base = 1.60
        else:
            base = 1.10
    return round(base + boost, 2)


def _normalise_market(model: str, raw_market: str, side: str = "",
                      team: str = "", favourite: str = "") -> str | None:
    """Map model-specific market labels to a unified SportyBet market key."""
    if model == "poisson":
        parts = raw_market.split()
        direction = parts[0].capitalize()
        line = parts[1]
        return f"Match {direction} {line}"

    if model == "mismatch":
        if "over_1.5" in raw_market or "Over 1.5" in raw_market:
            return "Match Over 1.5"
        if "over_2.5" in raw_market or "Over 2.5" in raw_market:
            return "Match Over 2.5"
        if raw_market == "fav_to_score":
            return f"{favourite} Over 0.5"
        if raw_market == "fav_over_1.5":
            return f"{favourite} Over 1.5"
        if raw_market == "fav_win":
            return f"{favourite} Win"
        return None

    if model == "bayesian":
        event = raw_market.split()[-1] if " " in raw_market else raw_market
        if event == "Over_1_5":
            return "Match Over 1.5"
        if event == "Over_2_5":
            return "Match Over 2.5"
        if event == "Over_3_5":
            return "Match Over 3.5"
        if event == "Under_2_5":
            return "Match Under 2.5"
        if event == "Under_3_5":
            return "Match Under 3.5"
        if event == "score_1goal":
            return f"{team} Over 0.5"
        if event == "score_2goals":
            return f"{team} Over 1.5"
        if event == "win":
            return f"{team} Win"
        if event == "unbeaten":
            return f"{team} Double Chance"
        if event == "concede_1":
            opp_side = "away" if side == "home" else "home"
            return f"OPP_{opp_side} Over 0.5"
        if event == "BTTS":
            return "BTTS Yes"
        if event == "clean_sheet":
            return f"{team} Clean Sheet"
        return None

    return None


def _sportify_market(unified: str, home: str, away: str) -> dict:
    """Convert a unified market key into SportyBet-bookable description."""
    if unified.startswith("Match Over"):
        line = unified.split()[-1]
        return {"market": "Over/Under", "selection": f"Over {line}", "line": float(line)}
    if unified.startswith("Match Under"):
        line = unified.split()[-1]
        return {"market": "Over/Under", "selection": f"Under {line}", "line": float(line)}
    if "Over 0.5" in unified:
        if unified.startswith("OPP_home"):
            return {"market": "Home Team Over/Under", "selection": f"{home} Over 0.5", "line": 0.5}
        if unified.startswith("OPP_away"):
            return {"market": "Away Team Over/Under", "selection": f"{away} Over 0.5", "line": 0.5}
        for team_name in (home, away):
            if team_name.lower() in unified.lower() or unified.startswith(team_name.split()[0]):
                is_home = team_name == home
                mkt = "Home Team Over/Under" if is_home else "Away Team Over/Under"
                return {"market": mkt, "selection": f"{team_name} Over 0.5", "line": 0.5}
        return {"market": "Team Over/Under", "selection": unified, "line": 0.5}
    if "Over 1.5" in unified and not unified.startswith("Match"):
        for team_name in (home, away):
            if team_name.lower() in unified.lower():
                is_home = team_name == home
                mkt = "Home Team Over/Under" if is_home else "Away Team Over/Under"
                return {"market": mkt, "selection": f"{team_name} Over 1.5", "line": 1.5}
    if "Win" in unified:
        return {"market": "1X2", "selection": unified, "line": None}
    if "Double Chance" in unified:
        for team_name in (home, away):
            if team_name.lower() in unified.lower():
                if team_name == home:
                    return {"market": "Double Chance", "selection": "1X", "line": None}
                else:
                    return {"market": "Double Chance", "selection": "X2", "line": None}
        return {"market": "Double Chance", "selection": unified, "line": None}
    if "BTTS" in unified:
        return {"market": "Both Teams to Score", "selection": "Yes", "line": None}
    return {"market": "Other", "selection": unified, "line": None}


def run_consensus(days: int = 10, target_odds: float = TARGET_ODDS,
                  bankroll: float = 0) -> dict:
    """Run all three models and build consensus picks."""

    # ── 1. Poisson GLM ──────────────────────────────────────────────────
    print("=" * 60)
    print("  [1/3] POISSON GLM")
    print("=" * 60)
    poisson_picks = predict_upcoming(days=days)

    # ── 2. Mismatch Elo ─────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  [2/3] MISMATCH ELO")
    print("=" * 60)
    mismatch_picks, fixture_strength = predict_mismatch(days=days)

    # ── 3. Bayesian Streaks ──────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  [3/3] BAYESIAN STREAKS")
    print("=" * 60)
    from engine.eval.bayesian_streaks import _get_conn
    conn = _get_conn()
    bayesian_by_fixture = get_fixture_beliefs(conn, days=days)
    conn.close()
    print(f"  {len(bayesian_by_fixture)} fixtures with Bayesian beliefs")

    # ── 4. Build unified pick pool ───────────────────────────────────────
    print()
    print("=" * 60)
    print("  BUILDING CONSENSUS")
    print("=" * 60)

    # Collect fixture metadata
    fixture_meta = {}

    # From Poisson
    if not poisson_picks.empty:
        for _, row in poisson_picks.iterrows():
            fid = int(row["fixture_id"])
            if fid not in fixture_meta:
                fixture_meta[fid] = {
                    "fixture_id": fid,
                    "home": row["home"],
                    "away": row["away"],
                    "league": row["league"],
                    "kickoff": str(row["kickoff"]),
                }

    # From Mismatch
    for p in mismatch_picks:
        fid = p.fixture_id
        if fid not in fixture_meta:
            fixture_meta[fid] = {
                "fixture_id": fid,
                "home": p.home_team,
                "away": p.away_team,
                "league": p.league,
                "kickoff": str(p.kickoff),
            }

    # From Bayesian (need fixture info from DB)
    conn = _get_conn()
    if bayesian_by_fixture:
        fids = list(bayesian_by_fixture.keys())
        fid_str = ",".join(str(f) for f in fids)
        fix_df = pd.read_sql(f"""
            SELECT fixture_id, team_name AS home, opponent_name AS away,
                   league_name AS league, kickoff_utc AS kickoff
            FROM raw.fact_head_to_head
            WHERE fixture_id IN ({fid_str}) AND is_home
        """, conn)
        for _, row in fix_df.iterrows():
            fid = int(row["fixture_id"])
            if fid not in fixture_meta:
                fixture_meta[fid] = {
                    "fixture_id": fid,
                    "home": row["home"],
                    "away": row["away"],
                    "league": row["league"],
                    "kickoff": str(row["kickoff"]),
                }

    # Also load upcoming unplayed fixtures for Bayesian-only matches
    upcoming_df = pd.read_sql("""
        SELECT fixture_id, home_team_name AS home, away_team_name AS away,
               kickoff_utc AS kickoff, api_league_id
        FROM raw.raw_fixtures
        WHERE fixture_status = 'NS'
        ORDER BY kickoff_utc
    """, conn)
    league_map_df = pd.read_sql("""
        SELECT DISTINCT r.api_league_id, f.league_name AS league
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head f ON f.fixture_id = r.fixture_id AND f.is_home
    """, conn)
    lmap = dict(zip(league_map_df["api_league_id"], league_map_df["league"]))

    # Build api_league_id -> country from metadata.yaml
    import yaml
    _meta_path = Path(__file__).resolve().parents[2] / "data" / "metadata.yaml"
    _league_id_country = {}
    if _meta_path.exists():
        with open(_meta_path) as _f:
            _meta = yaml.safe_load(_f)
        _name_countries = {}
        for entry in _meta.get("countries", []):
            for lg in entry.get("leagues", []):
                _name_countries.setdefault(lg["name"], []).append(entry["country"])
        for lid, lname in lmap.items():
            countries = _name_countries.get(lname, [])
            _league_id_country[lid] = countries[0] if len(countries) == 1 else None

        # Disambiguate by matching teams to known countries
        if not all(_league_id_country.values()):
            _LEAGUE_ID_COUNTRY_OVERRIDE = {
                39: "England", 116: "Belarus", 233: "Egypt", 235: "Russia",
                104: "Norway", 117: "Belarus",
                72: "Brazil", 136: "Italy",
            }
            for lid, country in _LEAGUE_ID_COUNTRY_OVERRIDE.items():
                if lid in _league_id_country:
                    _league_id_country[lid] = country

    # Map fixture_id -> api_league_id for country lookup
    fid_league_id = dict(zip(upcoming_df["fixture_id"].astype(int),
                             upcoming_df["api_league_id"]))

    for _, row in upcoming_df.iterrows():
        fid = int(row["fixture_id"])
        if fid not in fixture_meta and fid in bayesian_by_fixture:
            fixture_meta[fid] = {
                "fixture_id": fid,
                "home": row["home"],
                "away": row["away"],
                "league": lmap.get(row.get("api_league_id", ""), "Unknown"),
                "kickoff": str(row["kickoff"]),
            }

    # Attach country to all fixture metadata
    # First get api_league_id for fixtures already in meta (from Poisson/Mismatch)
    all_rf = pd.read_sql("""
        SELECT fixture_id, api_league_id FROM raw.raw_fixtures
    """, conn)
    all_fid_lid = dict(zip(all_rf["fixture_id"].astype(int), all_rf["api_league_id"]))
    for fid, meta in fixture_meta.items():
        lid = all_fid_lid.get(fid)
        meta["country"] = _league_id_country.get(lid, "") if lid else ""

    conn.close()

    # ── Build per-fixture, per-market pick objects ───────────────────────
    # Key: (fixture_id, unified_market) → {models: [...], max_conf, ...}
    consensus = {}

    def _add_pick(fid, unified, model, confidence, detail=None):
        key = (fid, unified)
        if key not in consensus:
            consensus[key] = {
                "fixture_id": fid,
                "unified_market": unified,
                "models": [],
                "confidences": [],
                "details": [],
            }
        consensus[key]["models"].append(model)
        consensus[key]["confidences"].append(confidence)
        if detail:
            consensus[key]["details"].append(detail)

    # Poisson picks
    if not poisson_picks.empty:
        for _, row in poisson_picks.iterrows():
            fid = int(row["fixture_id"])
            raw_mkt = f"{row['direction']} {row['line']}"
            unified = _normalise_market("poisson", raw_mkt)
            if unified:
                _add_pick(fid, unified, "poisson", row["probability"],
                          {"tier": _tier(row["probability"]), "raw": row.get("raw_probability")})

    # Mismatch picks
    for p in mismatch_picks:
        unified = _normalise_market("mismatch", p.market, favourite=p.favourite)
        if unified:
            _add_pick(p.fixture_id, unified, "mismatch", p.confidence,
                      {"gap": p.gap, "gap_bin": p.gap_bin, "h2h_boost": p.h2h_boost})

    # Bayesian picks
    for fid, sides in bayesian_by_fixture.items():
        meta = fixture_meta.get(fid)
        if not meta:
            continue
        for side in ("home", "away"):
            for b in sides.get(side, []):
                team = b["team"]
                event = b["event"]
                raw_mkt = f"{team} {event}"
                unified = _normalise_market("bayesian", raw_mkt, side=side,
                                           team=team)
                if unified:
                    if unified.startswith("OPP_"):
                        opp_side = "home" if side == "away" else "away"
                        opp_team = meta["home"] if opp_side == "home" else meta["away"]
                        unified = f"{opp_team} Over 0.5"
                    _add_pick(fid, unified, "bayesian", b["posterior"],
                              {"streak": b["streak"], "trend": b["trend"],
                               "n": b["n"], "base_rate": b["base_rate"]})

    # ── Score and rank ──────────────────────────────────────────────────
    ranked = []
    for key, pick in consensus.items():
        fid = pick["fixture_id"]
        meta = fixture_meta.get(fid)
        if not meta:
            continue

        n_models = len(set(pick["models"]))
        max_conf = max(pick["confidences"])
        avg_conf = sum(pick["confidences"]) / len(pick["confidences"])

        # Filter by tier thresholds
        if n_models >= 3 and avg_conf < TRIO_THRESHOLD:
            continue
        if n_models == 2 and avg_conf < DUO_THRESHOLD:
            continue
        if n_models == 1 and max_conf < SOLO_THRESHOLD:
            continue

        est_odds = _est_odds(avg_conf, pick["unified_market"])
        if est_odds < MIN_EST_ODDS:
            continue

        # Score: model count + confidence + odds bonus (prefer bettable picks)
        odds_bonus = min((est_odds - 1.0) * 0.5, 0.3)
        score = n_models * 1.0 + avg_conf + odds_bonus

        sportify = _sportify_market(pick["unified_market"], meta["home"], meta["away"])

        ranked.append({
            "fixture_id": fid,
            "match": f"{meta['home']} vs {meta['away']}",
            "home": meta["home"],
            "away": meta["away"],
            "league": meta["league"],
            "kickoff": meta["kickoff"],
            "unified_market": pick["unified_market"],
            "sportybet_market": sportify["market"],
            "sportybet_selection": sportify["selection"],
            "line": sportify.get("line"),
            "n_models": n_models,
            "models": sorted(set(pick["models"])),
            "max_confidence": round(max_conf, 4),
            "avg_confidence": round(avg_conf, 4),
            "consensus_score": round(score, 4),
            "est_odds": est_odds,
            "country": meta.get("country", ""),
            "details": pick["details"],
        })

    ranked.sort(key=lambda x: (-x["n_models"], -x["consensus_score"]))

    print(f"  {len(ranked)} consensus picks after filtering")
    by_tier = {3: 0, 2: 0, 1: 0}
    for r in ranked:
        by_tier[r["n_models"]] = by_tier.get(r["n_models"], 0) + 1
    print(f"  3-model: {by_tier.get(3,0)} | 2-model: {by_tier.get(2,0)} | 1-model (P≥{SOLO_THRESHOLD:.0%}): {by_tier.get(1,0)}")

    # ── Per-fixture analysis (all raw model signals) ──────────────────
    fixture_analysis = {}
    for fid, meta in fixture_meta.items():
        fixture_analysis[fid] = {**meta, "poisson": [], "mismatch": [], "bayesian": []}

    if not poisson_picks.empty:
        for _, row in poisson_picks.iterrows():
            fid = int(row["fixture_id"])
            if fid in fixture_analysis:
                fixture_analysis[fid]["poisson"].append({
                    "market": f"{row['direction']} {row['line']}",
                    "probability": round(float(row["probability"]), 4),
                    "tier": _tier(row["probability"]),
                })

    for p in mismatch_picks:
        fid = p.fixture_id
        if fid in fixture_analysis:
            entry = {
                "market": p.market,
                "label": _market_label(p.market, p.favourite),
                "confidence": round(p.confidence, 4),
                "favourite": p.favourite,
                "gap": p.gap,
                "gap_bin": p.gap_bin,
            }
            if p.h2h_boost:
                entry["h2h"] = p.h2h_boost
            if hasattr(p, "fav_attack_pct") and p.fav_attack_pct:
                entry["fav_atk"] = p.fav_attack_pct
            if hasattr(p, "dog_defense_pct") and p.dog_defense_pct:
                entry["dog_def"] = p.dog_defense_pct
            fixture_analysis[fid]["mismatch"].append(entry)

    for fid, sides in bayesian_by_fixture.items():
        if fid not in fixture_analysis:
            continue
        for side in ("home", "away"):
            for b in sides.get(side, []):
                fixture_analysis[fid]["bayesian"].append({
                    "team": b["team"],
                    "side": side,
                    "event": b["event"],
                    "posterior": round(b["posterior"], 4),
                    "base_rate": round(b["base_rate"], 4),
                    "n": b["n"],
                    "streak": b["streak"],
                    "trend": b["trend"],
                })

    # Compute team strength for ALL fixtures (not just Mismatch-covered ones)
    conn = _get_conn()
    try:
        strength_df = load_team_df(conn)
        strength_all = compute_team_strength(strength_df)

        # Build h2h_team_id -> strength profile lookup
        strength_by_h2h = {}
        for _, row in strength_all.iterrows():
            profile = {
                "overall": {
                    "attack_shrunk": round(float(row["attack_shrunk"]), 3),
                    "defense_shrunk": round(float(row["defense_shrunk"]), 3),
                    "attack_pct": round(float(row["attack_percentile"]), 1),
                    "defense_pct": round(float(row["defense_percentile"]), 1),
                    "attack_rank": int(row["attack_rank"]),
                    "defense_rank": int(row["defense_rank"]),
                    "league_size": int(row["league_size"]),
                },
            }
            for ctx in ("home", "away"):
                col_atk = f"{ctx}_attack_shrunk"
                if col_atk in row.index and pd.notna(row.get(col_atk)):
                    profile[ctx] = {
                        "attack_shrunk": round(float(row[f"{ctx}_attack_shrunk"]), 3),
                        "defense_shrunk": round(float(row[f"{ctx}_defense_shrunk"]), 3),
                        "attack_pct": round(float(row[f"{ctx}_attack_percentile"]), 1),
                        "defense_pct": round(float(row[f"{ctx}_defense_percentile"]), 1),
                        "attack_rank": int(row[f"{ctx}_attack_rank"]) if pd.notna(row.get(f"{ctx}_attack_rank")) else None,
                        "defense_rank": int(row[f"{ctx}_defense_rank"]) if pd.notna(row.get(f"{ctx}_defense_rank")) else None,
                        "league_size": int(row[f"{ctx}_league_size"]) if pd.notna(row.get(f"{ctx}_league_size")) else None,
                    }
                else:
                    profile[ctx] = None
            strength_by_h2h[row["team_id"]] = profile

        # Build rf_team_id -> h2h_team_id map for all fixtures
        all_rf_ids = set()
        rf_fixtures = pd.read_sql("""
            SELECT fixture_id, home_team_id, away_team_id
            FROM raw.raw_fixtures
        """, conn)
        rf_lookup = {}
        for _, r in rf_fixtures.iterrows():
            rf_lookup[int(r["fixture_id"])] = (int(r["home_team_id"]), int(r["away_team_id"]))
            all_rf_ids.add(int(r["home_team_id"]))
            all_rf_ids.add(int(r["away_team_id"]))

        rf_id_list = ",".join(str(i) for i in all_rf_ids)
        id_map_df = pd.read_sql(f"""
            SELECT DISTINCT r.home_team_id AS rf_id, h.team_id AS h2h_id
            FROM raw.raw_fixtures r
            JOIN raw.fact_head_to_head h ON h.fixture_id = r.fixture_id AND h.is_home
            WHERE r.home_team_id IN ({rf_id_list})
            UNION
            SELECT DISTINCT r.away_team_id AS rf_id, h.team_id AS h2h_id
            FROM raw.raw_fixtures r
            JOIN raw.fact_head_to_head h ON h.fixture_id = r.fixture_id AND NOT h.is_home
            WHERE r.away_team_id IN ({rf_id_list})
        """, conn)
        rf_to_h2h = dict(zip(id_map_df["rf_id"], id_map_df["h2h_id"]))

        for fid, fa in fixture_analysis.items():
            rf_ids = rf_lookup.get(fid)
            if not rf_ids:
                fa["strength"] = None
                continue
            home_rf, away_rf = rf_ids
            home_h2h = rf_to_h2h.get(home_rf)
            away_h2h = rf_to_h2h.get(away_rf)
            fa["strength"] = {
                "home_team": strength_by_h2h.get(home_h2h) if home_h2h else None,
                "away_team": strength_by_h2h.get(away_h2h) if away_h2h else None,
            }
    finally:
        conn.close()

    analysis_list = [v for v in fixture_analysis.values()
                     if v["poisson"] or v["mismatch"] or v["bayesian"]]
    analysis_list.sort(key=lambda x: x.get("kickoff", ""))

    # ── Assemble slips ──────────────────────────────────────────────────
    slips = _build_slips(ranked, target_odds, bankroll=bankroll)
    return {"pool": ranked, "slips": slips, "fixture_analysis": analysis_list}


def _build_slips(ranked: list[dict], target_odds: float,
                 bankroll: float = 0, max_legs: int = MAX_LEGS) -> list[dict]:
    """Build multiple short (3-5 leg) slips from the ranked pool.

    Prioritises duo/trio consensus, diversifies across fixtures,
    and assigns stakes based on slip confidence tier.
    """

    def _slip_tier(selections):
        avg_models = sum(s["n_models"] for s in selections) / len(selections)
        avg_conf = sum(s["avg_confidence"] for s in selections) / len(selections)
        if avg_models >= 2.5 and avg_conf >= 0.80:
            return "A"
        if avg_models >= 2.0 and avg_conf >= 0.75:
            return "B"
        return "C"

    WEIGHT = {"A": 3, "B": 2, "C": 1}
    MAX_SLIPS = 6

    used_globally = set()
    raw_slips = []
    remaining = list(ranked)

    while remaining and len(raw_slips) < MAX_SLIPS:
        slip = []
        cum_odds = 1.0
        used_fixtures = set()

        for pick in remaining:
            if len(slip) >= max_legs:
                break
            fid = pick["fixture_id"]
            if fid in used_fixtures:
                continue
            slip.append(pick)
            used_fixtures.add(fid)
            cum_odds *= pick["est_odds"]

        if len(slip) < MIN_LEGS:
            break

        tier = _slip_tier(slip)
        raw_slips.append((slip, cum_odds, tier, used_fixtures))

        used_globally.update(used_fixtures)
        remaining = [p for p in remaining
                     if p["fixture_id"] not in used_globally]

    if not raw_slips or bankroll <= 0:
        return [{"name": f"Slip {i+1} (Tier {t})", "selections": s,
                 "est_total_odds": round(o, 2), "legs": len(s),
                 "tier": t, "stake": 0, "potential_return": 0}
                for i, (s, o, t, _) in enumerate(raw_slips)]

    total_weight = sum(WEIGHT[t] for _, _, t, _ in raw_slips)
    slips = []
    for i, (sel, odds, tier, _) in enumerate(raw_slips):
        stake = round(bankroll * WEIGHT[tier] / total_weight)
        slips.append({
            "name": f"Slip {i+1} (Tier {tier})",
            "selections": sel,
            "est_total_odds": round(odds, 2),
            "legs": len(sel),
            "tier": tier,
            "stake": stake,
            "potential_return": round(stake * odds),
        })

    return slips


def print_slips(result: dict) -> None:
    """Pretty-print the consensus slips."""
    pool = result["pool"]
    slips = result["slips"]

    print(f"\n{'='*120}")
    print(f"  CONSENSUS SLIPS — Short-form accumulators (3-5 legs)")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Pool: {len(pool)} qualified picks → {len(slips)} slips")
    print(f"{'='*120}")

    total_stake = sum(s.get("stake", 0) for s in slips)
    total_potential = sum(s.get("potential_return", 0) for s in slips)
    if total_stake > 0:
        print(f"  Total stake: ₦{total_stake:,.0f}  |  Max potential: ₦{total_potential:,.0f}")

    for slip in slips:
        sels = slip["selections"]
        print(f"\n{'─'*120}")
        header = f"  {slip['name']}  |  {slip['legs']} legs  |  odds: {slip['est_total_odds']:.1f}x"
        if slip.get("stake"):
            header += f"  |  stake: ₦{slip['stake']:,.0f}  →  ₦{slip['potential_return']:,.0f}"
        print(header)
        print(f"{'─'*120}")
        print(f"  {'#':>2s}  {'Models':>6s}  {'Conf':>5s}  {'Odds':>5s}  {'Kickoff':>18s}  {'Match':<40s}  {'Selection':<28s}  League")
        print(f"  {'─'*2}  {'─'*6}  {'─'*5}  {'─'*5}  {'─'*18}  {'─'*40}  {'─'*28}  {'─'*20}")

        for i, sel in enumerate(sels, 1):
            models_str = "+".join(m[0].upper() for m in sel["models"])
            try:
                dt = pd.Timestamp(sel["kickoff"]).strftime("%a %b %d %H:%M")
            except Exception:
                dt = sel["kickoff"][:16]
            print(f"  {i:>2d}  {models_str:>6s}  {sel['avg_confidence']:>4.0%}  {sel['est_odds']:>5.2f}  "
                  f"{dt:>18s}  {sel['match']:<40s}  {sel['sportybet_selection']:<28s}  {sel['league']}")

        print()


def save_slips(result: dict) -> str:
    ds = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = Path(__file__).parent / f"consensus_slips_{ds}.json"

    pool_items = []
    for p in result["pool"]:
        pool_items.append({
            "fixture_id": p["fixture_id"],
            "match": p["match"],
            "home": p["home"],
            "away": p["away"],
            "kickoff": p["kickoff"],
            "league": p["league"],
            "sportybet_market": p["sportybet_market"],
            "sportybet_selection": p["sportybet_selection"],
            "line": p.get("line"),
            "n_models": p["n_models"],
            "models": p["models"],
            "avg_confidence": p["avg_confidence"],
            "max_confidence": p["max_confidence"],
            "est_odds": p["est_odds"],
            "country": p.get("country", ""),
        })

    output = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "models": ["poisson_glm", "mismatch_elo", "bayesian_streaks"],
        "thresholds": {
            "solo": SOLO_THRESHOLD,
            "duo": DUO_THRESHOLD,
            "trio": TRIO_THRESHOLD,
        },
        "pool_size": len(result["pool"]),
        "pool": pool_items,
        "slips": [],
    }

    for slip in result["slips"]:
        slip_data = {
            "name": slip["name"],
            "legs": slip["legs"],
            "est_total_odds": slip["est_total_odds"],
            "tier": slip.get("tier", ""),
            "stake": slip.get("stake", 0),
            "potential_return": slip.get("potential_return", 0),
            "selections": [],
        }
        for sel in slip["selections"]:
            slip_data["selections"].append({
                "match": sel["match"],
                "home": sel["home"],
                "away": sel["away"],
                "kickoff": sel["kickoff"],
                "league": sel["league"],
                "sportybet_market": sel["sportybet_market"],
                "sportybet_selection": sel["sportybet_selection"],
                "line": sel.get("line"),
                "n_models": sel["n_models"],
                "models": sel["models"],
                "avg_confidence": sel["avg_confidence"],
                "est_odds": sel["est_odds"],
            })
        output["slips"].append(slip_data)

    output["fixture_analysis"] = result.get("fixture_analysis", [])

    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, default=str)

    print(f"\nConsensus slips saved to {out_path}")
    return str(out_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-model consensus slip generator")
    parser.add_argument("--days", type=int, default=10, help="Days ahead to scan")
    parser.add_argument("--target-odds", type=float, default=5.0,
                        help="Target total odds per slip")
    parser.add_argument("--bankroll", type=float, default=0,
                        help="Total bankroll for stake sizing (e.g. 300000)")
    args = parser.parse_args()

    result = run_consensus(days=args.days, target_odds=args.target_odds,
                           bankroll=args.bankroll)
    print_slips(result)
    if result["slips"]:
        save_slips(result)
