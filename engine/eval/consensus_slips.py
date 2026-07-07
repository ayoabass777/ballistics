"""
Consensus slip generator — multi-model 10x betting slips.

Runs all four engines (Poisson GLM, Mismatch Elo, Bayesian Streaks,
Dixon-Coles), maps every qualifying pick to a unified SportyBet market,
scores each pick by how many models agree, and assembles two ~10x
accumulator slips.

Model-to-market mapping:
    Poisson  OVER 2.5 / UNDER 3.5       →  Match Over 2.5 / Under 3.5
    Mismatch fav_to_score                →  Team Over 0.5 (fav side)
    Mismatch over_1.5 / over_2.5         →  Match Over 1.5 / Over 2.5
    Bayesian Over_1_5 / Over_2_5 / etc.  →  Match Over 1.5 / Over 2.5
    Bayesian score_1goal                 →  Team Over 0.5
    Bayesian concede_1                   →  Opponent Over 0.5
    Bayesian Under_2_5 / Under_3_5       →  Match Under 2.5 / Under 3.5
    Dixon-Coles (markets already unified) →  Match/Team O/U, BTTS Yes, Win

Consensus tiers (n = distinct models agreeing, 1-4):
    3+ models →  backbone (trio+ — three or more agree)
    2-model  →  strong (two independent signals)
    1-model  →  solo (only if P >= solo_threshold)

Goal-model rules (walk-forward validated, folds 5-9):
    - Match Over 1.5 requires BOTH Dixon-Coles AND Poisson to agree,
      with calibrated consensus P >= 78%.
    - Under selections require BOTH Dixon-Coles AND Poisson to agree.
    - Solo goal-model match-total picks are dropped (DC solo unders 53.3%).
    - BTTS Yes is sourced from Dixon-Coles only; Bayesian BTTS is ignored.

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
from engine.eval.predict_dixon_coles import predict_dixon_coles
from engine.eval.scrape_sportybet import odds_for_pick
from engine.eval.db import get_conn
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
OVER15_DC_POISSON_THRESHOLD = 0.78
UNDER35_MAX_AVG_ATTACK_PCT = 55.0
UNDER35_MAX_SINGLE_ATTACK_PCT = 75.0
UNDER35_MIN_AVG_DEFENSE_PCT = 55.0
UNDER35_MIN_SINGLE_DEFENSE_PCT = 35.0
UNDER35_TIER_A_THRESHOLD = 0.80
UNDER35_TIER_B_THRESHOLD = 0.75
UNDER35_TIER_C_THRESHOLD = 0.72

# In high-scoring leagues Poisson overpredicts unders (+4.4% walk-forward gap,
# because its per-cluster fit pools lower-scoring leagues) while Dixon-Coles —
# fit per league — stays calibrated (+0.6%). So Match Under markets in these
# leagues are priced off DC alone instead of the equal DC+Poisson blend.
HIGH_SCORING_AVG_GOALS = 3.1     # league goals/game above which to favour DC on unders
UNDER_HIGH_DC_WEIGHT = 1.0       # weight on DC's prob there (1.0 = DC-only)

TARGET_ODDS = 5.0
MAX_LEGS = 5
MIN_LEGS = 3
MIN_EST_ODDS = 1.15
MIN_EV = 1.0            # slip legs must clear break-even EV (model_prob × real_odds)

# Leagues SportyBet-NG does not carry — cut from the bettable pool so the
# engine only optimises markets that can actually be booked. Verified against
# the odds API on July 3 2026 by mapping each (league, country) to SportyBet's
# own tournament name (which often differs, e.g. our "League One"/China is
# SportyBet "China League 1", "Premier League"/Belarus is "Vysshaya Liga"),
# so exact league-name matching is NOT reliable — match on (league, country).
# Only the Belarusian 2nd tier (our "1. Division"/Belarus) had no SportyBet
# equivalent; everything else is carried under a renamed tournament.
# TODO: once scrape_sportybet lands, drive bookability from actual odds
# presence instead of this static list. See memory sportybet-odds-coverage.
UNBOOKABLE_LEAGUES = {
    ("1. Division", "Belarus"),
}


def _is_bookable(league: str, country: str = "") -> bool:
    """True unless the (league, country) is one SportyBet-NG does not offer."""
    return (league, country or "") not in UNBOOKABLE_LEAGUES

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


def _load_sportybet_odds() -> dict:
    """Read raw.sportybet_odds into {fixture_id: markets} for odds_for_pick().

    markets = {"OU": {line: {Over,Under}}, "HOME_OU": ..., "AWAY_OU": ...,
               "1X2": {Home,Draw,Away}, "DC": {...}, "BTTS": {...}}.
    Returns {} if the table is absent or empty (engine then uses est_odds).
    """
    from engine.eval.bayesian_streaks import _get_conn
    from collections import defaultdict
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('raw.sportybet_odds')")
        if cur.fetchone()[0] is None:
            return {}
        cur.execute("""
            SELECT fixture_id, market, line, selection, decimal_odds
            FROM raw.sportybet_odds
        """)
        tiered: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        flat: dict = defaultdict(lambda: defaultdict(dict))
        for fid, market, line, sel, od in cur.fetchall():
            fid = int(fid)
            od = float(od)
            if market in ("OU", "HOME_OU", "AWAY_OU"):
                tiered[fid][market][line or ""][sel] = od
            else:
                flat[fid][market][sel] = od
    except Exception:  # noqa: BLE001
        return {}
    finally:
        conn.close()

    out = {}
    for fid in set(tiered) | set(flat):
        m = {k: {ln: dict(v) for ln, v in d.items()} for k, d in tiered.get(fid, {}).items()}
        m.update({k: dict(v) for k, v in flat.get(fid, {}).items()})
        out[fid] = m
    return out


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

    if model == "dixon_coles":
        return raw_market

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


def _is_match_total_market(unified: str) -> bool:
    return unified.startswith("Match Over") or unified.startswith("Match Under")


def _passes_goal_model_rules(unified: str, models: set[str]) -> bool:
    if unified == "Match Over 1.5":
        return {"poisson", "dixon_coles"}.issubset(models)
    if unified.startswith("Match Under"):
        return {"poisson", "dixon_coles"}.issubset(models)
    if _is_match_total_market(unified) and len(models) == 1:
        return False
    if unified == "BTTS Yes":
        return "dixon_coles" in models
    return True


def _strength_profile(row: pd.Series) -> dict:
    profile = {
        "overall": {
            "attack_rate": round(float(row["attack_rate"]), 2),
            "defense_rate": round(float(row["defense_rate"]), 2),
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
                "attack_rate": round(float(row[f"{ctx}_attack_rate"]), 2) if pd.notna(row.get(f"{ctx}_attack_rate")) else None,
                "defense_rate": round(float(row[f"{ctx}_defense_rate"]), 2) if pd.notna(row.get(f"{ctx}_defense_rate")) else None,
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
    return profile


def _load_fixture_strength_profiles(conn) -> dict[int, dict]:
    strength_df = load_team_df(conn)
    strength_all = compute_team_strength(strength_df)
    strength_by_h2h = {
        row["team_id"]: _strength_profile(row)
        for _, row in strength_all.iterrows()
    }

    rf_fixtures = pd.read_sql("""
        SELECT fixture_id, home_team_id, away_team_id
        FROM raw.raw_fixtures
    """, conn)
    if rf_fixtures.empty:
        return {}

    rf_lookup = {}
    all_rf_ids = set()
    for _, r in rf_fixtures.iterrows():
        fid = int(r["fixture_id"])
        home_rf = int(r["home_team_id"])
        away_rf = int(r["away_team_id"])
        rf_lookup[fid] = (home_rf, away_rf)
        all_rf_ids.add(home_rf)
        all_rf_ids.add(away_rf)

    rf_id_list = ",".join(str(i) for i in sorted(all_rf_ids))
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

    fixture_strength = {}
    for fid, (home_rf, away_rf) in rf_lookup.items():
        home_h2h = rf_to_h2h.get(home_rf)
        away_h2h = rf_to_h2h.get(away_rf)
        fixture_strength[fid] = {
            "home_team": strength_by_h2h.get(home_h2h) if home_h2h else None,
            "away_team": strength_by_h2h.get(away_h2h) if away_h2h else None,
        }
    return fixture_strength


def _ctx_strength(team_profile: dict | None, ctx: str) -> dict | None:
    if not team_profile:
        return None
    return team_profile.get(ctx) or team_profile.get("overall")


def _rank_summary(ctx: dict | None) -> dict | None:
    if not ctx:
        return None
    return {
        "attack_rank": ctx.get("attack_rank"),
        "defense_rank": ctx.get("defense_rank"),
        "league_size": ctx.get("league_size"),
        "attack_pct": ctx.get("attack_pct"),
        "defense_pct": ctx.get("defense_pct"),
    }


def _fixture_strength_rank(strength: dict | None) -> dict | None:
    if not strength:
        return None
    home_profile = strength.get("home_team")
    away_profile = strength.get("away_team")
    home_ctx = _ctx_strength(home_profile, "home")
    away_ctx = _ctx_strength(away_profile, "away")
    home_overall = _ctx_strength(home_profile, "overall")
    away_overall = _ctx_strength(away_profile, "overall")
    if not home_ctx and not away_ctx and not home_overall and not away_overall:
        return None
    return {
        "home": {
            "context": "home",
            "split": _rank_summary(home_ctx),
            "overall": _rank_summary(home_overall),
        },
        "away": {
            "context": "away",
            "split": _rank_summary(away_ctx),
            "overall": _rank_summary(away_overall),
        },
    }


def _under35_strength_gate(strength: dict | None) -> tuple[bool, dict | None]:
    if not strength:
        return False, None
    home = _ctx_strength(strength.get("home_team"), "home")
    away = _ctx_strength(strength.get("away_team"), "away")
    if not home or not away:
        return False, None

    attack_pcts = [home["attack_pct"], away["attack_pct"]]
    defense_pcts = [home["defense_pct"], away["defense_pct"]]
    avg_attack = sum(attack_pcts) / 2
    avg_defense = sum(defense_pcts) / 2
    detail = {
        "avg_attack_pct": round(avg_attack, 1),
        "max_attack_pct": round(max(attack_pcts), 1),
        "avg_defense_pct": round(avg_defense, 1),
        "min_defense_pct": round(min(defense_pcts), 1),
    }
    ok = (
        avg_attack <= UNDER35_MAX_AVG_ATTACK_PCT
        and max(attack_pcts) <= UNDER35_MAX_SINGLE_ATTACK_PCT
        and avg_defense >= UNDER35_MIN_AVG_DEFENSE_PCT
        and min(defense_pcts) >= UNDER35_MIN_SINGLE_DEFENSE_PCT
    )
    return ok, detail


def _market_tier(unified_market: str, avg_confidence: float,
                 strength_gate: dict | None = None) -> str | None:
    """Market-specific quality tier from walk-forward calibration."""
    if unified_market != "Match Under 3.5" or not strength_gate:
        return None
    if avg_confidence >= UNDER35_TIER_A_THRESHOLD:
        return "A"
    if avg_confidence >= UNDER35_TIER_B_THRESHOLD:
        return "B"
    if avg_confidence >= UNDER35_TIER_C_THRESHOLD:
        return "C"
    return None


def _f(v):
    """Coerce numpy/NaN/None to a plain float or None (JSON/DB safe)."""
    if v is None:
        return None
    try:
        return float(v) if pd.notna(v) else None
    except (TypeError, ValueError):
        return None


# ── per-model signal extraction (shared by run_consensus and run_model) ───────
# Each returns a list of normalised picks:
#   {fixture_id, model, unified_market, probability, raw_probability, detail}

def poisson_signals(poisson_picks) -> list[dict]:
    out = []
    if poisson_picks is None or poisson_picks.empty:
        return out
    for _, row in poisson_picks.iterrows():
        unified = _normalise_market("poisson", f"{row['direction']} {row['line']}")
        if unified:
            out.append({"fixture_id": int(row["fixture_id"]), "model": "poisson",
                        "unified_market": unified, "probability": _f(row["probability"]),
                        "raw_probability": _f(row.get("raw_probability")),
                        "detail": {"tier": _tier(row["probability"]),
                                   "raw": _f(row.get("raw_probability"))}})
    return out


def mismatch_signals(mismatch_picks) -> list[dict]:
    out = []
    for p in mismatch_picks:
        unified = _normalise_market("mismatch", p.market, favourite=p.favourite)
        if unified:
            out.append({"fixture_id": int(p.fixture_id), "model": "mismatch",
                        "unified_market": unified, "probability": _f(p.confidence),
                        "raw_probability": None,
                        "detail": {"gap": p.gap, "gap_bin": p.gap_bin,
                                   "h2h_boost": p.h2h_boost}})
    return out


def bayesian_signals(bayesian_by_fixture, home_away: dict) -> list[dict]:
    """home_away: {fixture_id: (home_name, away_name)} — for the concede→OPP remap."""
    out = []
    for fid, sides in bayesian_by_fixture.items():
        ha = home_away.get(fid)
        for side in ("home", "away"):
            for b in sides.get(side, []):
                if b["event"] == "BTTS":
                    continue
                unified = _normalise_market("bayesian", f"{b['team']} {b['event']}",
                                            side=side, team=b["team"])
                if not unified:
                    continue
                if unified.startswith("OPP_"):
                    if not ha:
                        continue
                    opp_side = "home" if side == "away" else "away"
                    unified = f"{ha[0] if opp_side == 'home' else ha[1]} Over 0.5"
                out.append({"fixture_id": int(fid), "model": "bayesian",
                            "unified_market": unified, "probability": _f(b["posterior"]),
                            "raw_probability": None,
                            "detail": {"streak": b["streak"], "trend": b["trend"],
                                       "n": b["n"], "base_rate": b["base_rate"]}})
    return out


def dixon_signals(dixon_picks) -> list[dict]:
    out = []
    if dixon_picks is None or dixon_picks.empty:
        return out
    for _, row in dixon_picks.iterrows():
        unified = _normalise_market("dixon_coles", row["market"])
        if unified:
            out.append({"fixture_id": int(row["fixture_id"]), "model": "dixon_coles",
                        "unified_market": unified, "probability": _f(row["probability"]),
                        "raw_probability": _f(row.get("raw_probability")),
                        "detail": {"raw": _f(row.get("raw_probability")),
                                   "lambda_home": _f(row.get("lambda_home")),
                                   "lambda_away": _f(row.get("lambda_away"))}})
    return out


def _high_scoring_fixture_ids(fixture_ids, conn=None) -> set:
    """fixture_ids whose league averages > HIGH_SCORING_AVG_GOALS goals/game."""
    ids = [int(f) for f in fixture_ids]
    if not ids:
        return set()
    own = conn is None
    conn = conn or get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT rf.api_league_id, avg(f.goals_for + f.goals_against)
            FROM raw.fact_head_to_head f
            JOIN raw.raw_fixtures rf ON rf.fixture_id = f.fixture_id
            WHERE f.is_home AND f.is_played AND rf.season IN (2025, 2026)
            GROUP BY rf.api_league_id
        """)
        high_lids = {lid for lid, ag in cur.fetchall()
                     if ag is not None and float(ag) > HIGH_SCORING_AVG_GOALS}
        cur.execute("SELECT fixture_id, api_league_id FROM raw.raw_fixtures "
                    "WHERE fixture_id = ANY(%s)", (ids,))
        return {int(fid) for fid, lid in cur.fetchall() if lid in high_lids}
    except Exception:  # noqa: BLE001
        return set()
    finally:
        if own:
            conn.close()


def _score_pool(consensus: dict, fixture_meta: dict,
                fixture_strength_all: dict, odds_map: dict,
                high_scoring_fids: set = frozenset()) -> list[dict]:
    """Apply gates, tiers, odds/EV and rank the consensus dict into a pool.

    Pure function of its inputs — shared by the in-process `run_consensus`
    and the fan-in `build_consensus_from_signals` (DAG) path.
    """
    ranked = []
    gate_status = {}   # (fid, unified_market) -> {"in_pool": bool, "reason": str}
    for key, pick in consensus.items():
        fid = pick["fixture_id"]
        um = pick["unified_market"]
        meta = fixture_meta.get(fid)
        if not meta:
            continue

        def _reject(reason):
            gate_status[key] = {"in_pool": False, "reason": reason}

        # Drop leagues SportyBet-NG cannot book — keep the pool to bettable markets
        if not _is_bookable(meta["league"], meta.get("country", "")):
            _reject("not on SportyBet"); continue

        models = set(pick["models"])
        n_models = len(models)
        max_conf = max(pick["confidences"])
        avg_conf = sum(pick["confidences"]) / len(pick["confidences"])

        # DC-favoured unders in high-scoring leagues: Poisson overpredicts unders
        # there while DC stays calibrated, so price off DC's probability.
        if (um.startswith("Match Under")
                and fid in high_scoring_fids and "dixon_coles" in pick["models"]):
            dc_conf = pick["confidences"][pick["models"].index("dixon_coles")]
            avg_conf = UNDER_HIGH_DC_WEIGHT * dc_conf + (1 - UNDER_HIGH_DC_WEIGHT) * avg_conf

        strength_rank = _fixture_strength_rank(fixture_strength_all.get(fid))

        if not _passes_goal_model_rules(um, models):
            if um == "BTTS Yes":
                _reject("needs Dixon-Coles")
            elif um == "Match Over 1.5" or um.startswith("Match Under"):
                _reject("needs DC + Poisson")
            else:
                _reject("solo goal-model pick")
            continue
        if um == "Match Over 1.5" and avg_conf < OVER15_DC_POISSON_THRESHOLD:
            _reject(f"below Over 1.5 floor ({avg_conf:.0%} < {OVER15_DC_POISSON_THRESHOLD:.0%})")
            continue
        strength_gate = None
        if um == "Match Under 3.5":
            passes_strength, strength_gate = _under35_strength_gate(
                fixture_strength_all.get(fid))
            if not passes_strength:
                _reject("fails Under 3.5 strength gate"); continue
        market_tier = _market_tier(um, avg_conf, strength_gate)
        if um == "Match Under 3.5" and not market_tier:
            _reject("below Under 3.5 tier"); continue

        # Filter by tier thresholds
        if n_models >= 3 and avg_conf < TRIO_THRESHOLD:
            _reject(f"below trio threshold ({avg_conf:.0%} < {TRIO_THRESHOLD:.0%})"); continue
        if n_models == 2 and avg_conf < DUO_THRESHOLD:
            _reject(f"below duo threshold ({avg_conf:.0%} < {DUO_THRESHOLD:.0%})"); continue
        if n_models == 1 and max_conf < SOLO_THRESHOLD:
            _reject(f"below solo threshold ({max_conf:.0%} < {SOLO_THRESHOLD:.0%})"); continue

        est_odds = _est_odds(avg_conf, um)
        if est_odds < MIN_EST_ODDS:
            _reject("est odds too short"); continue

        gate_status[key] = {"in_pool": True, "reason": "in pool"}
        sportify = _sportify_market(um, meta["home"], meta["away"])

        # Real SportyBet odds + EV where available (else fall back to est_odds)
        real_odds = odds_for_pick(
            odds_map.get(fid, {}), sportify["market"], sportify["selection"],
            sportify.get("line"), meta["home"], meta["away"])
        has_odds = real_odds is not None
        book_odds = real_odds if has_odds else est_odds
        ev = round(avg_conf * real_odds, 4) if has_odds else None

        # Score by consensus strength then EV (real edge), not fabricated odds.
        # No odds yet → neutral EV proxy of 1.0 so it ranks below priced +EV legs.
        score = n_models * 1.0 + avg_conf + (ev - 1.0 if has_odds else 0.0)

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
            "real_odds": round(real_odds, 2) if has_odds else None,
            "has_odds": has_odds,
            "ev": ev,
            "odds": round(book_odds, 2),
            "country": meta.get("country", ""),
            "details": pick["details"],
            "strength_gate": strength_gate,
            "strength_rank": strength_rank,
            "market_tier": market_tier,
        })

    ranked.sort(key=lambda x: (-x["n_models"], -x["consensus_score"]))

    print(f"  {len(ranked)} consensus picks after filtering")
    by_tier = {4: 0, 3: 0, 2: 0, 1: 0}
    for r in ranked:
        by_tier[r["n_models"]] = by_tier.get(r["n_models"], 0) + 1
    print(f"  4-model: {by_tier.get(4,0)} | 3-model: {by_tier.get(3,0)} | "
          f"2-model: {by_tier.get(2,0)} | "
          f"1-model (P≥{SOLO_THRESHOLD:.0%}): {by_tier.get(1,0)}")
    return ranked, gate_status


def _league_id_country_map(conn) -> tuple[dict, dict]:
    """(api_league_id→league_name, api_league_id→country) from DB + metadata.yaml."""
    league_map_df = pd.read_sql("""
        SELECT DISTINCT r.api_league_id, f.league_name AS league
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head f ON f.fixture_id = r.fixture_id AND f.is_home
    """, conn)
    lmap = dict(zip(league_map_df["api_league_id"], league_map_df["league"]))

    import yaml
    lid_country = {}
    _meta_path = Path(__file__).resolve().parents[2] / "data" / "metadata.yaml"
    if _meta_path.exists():
        with open(_meta_path) as _f:
            _meta = yaml.safe_load(_f)
        name_countries = {}
        for entry in _meta.get("countries", []):
            for lg in entry.get("leagues", []):
                name_countries.setdefault(lg["name"], []).append(entry["country"])
        for lid, lname in lmap.items():
            cs = name_countries.get(lname, [])
            lid_country[lid] = cs[0] if len(cs) == 1 else None
        if not all(lid_country.values()):
            OVERRIDE = {39: "England", 116: "Belarus", 233: "Egypt", 235: "Russia",
                        104: "Norway", 117: "Belarus", 72: "Brazil", 136: "Italy"}
            for lid, c in OVERRIDE.items():
                if lid in lid_country:
                    lid_country[lid] = c
    return lmap, lid_country


def fixture_meta_from_db(fixture_ids, conn=None) -> dict:
    """{fid: {fixture_id, home, away, league, kickoff, country}} for given fixtures."""
    own = conn is None
    conn = conn or get_conn()
    try:
        ids = [int(f) for f in fixture_ids]
        if not ids:
            return {}
        lmap, lid_country = _league_id_country_map(conn)
        df = pd.read_sql("""
            SELECT fixture_id, home_team_name AS home, away_team_name AS away,
                   kickoff_utc AS kickoff, api_league_id
            FROM raw.raw_fixtures WHERE fixture_id = ANY(%(ids)s)
        """, conn, params={"ids": ids})
        meta = {}
        for _, r in df.iterrows():
            fid = int(r["fixture_id"])
            lid = r["api_league_id"]
            meta[fid] = {"fixture_id": fid, "home": r["home"], "away": r["away"],
                         "league": lmap.get(lid, "Unknown"), "kickoff": str(r["kickoff"]),
                         "country": lid_country.get(lid, "") or ""}
        return meta
    finally:
        if own:
            conn.close()


def _gates_by_fixture(gate_status: dict) -> dict:
    """(fid, market)->status  ==>  {fid: [{market, in_pool, reason}, ...]}."""
    out = {}
    for (fid, market), gs in gate_status.items():
        out.setdefault(fid, []).append({"market": market, **gs})
    for lst in out.values():
        lst.sort(key=lambda g: (not g["in_pool"], g["market"]))  # in-pool first
    return out


def _analysis_from_signals(signals: list[dict], fixture_meta: dict,
                           fixture_strength_all: dict,
                           gate_status: dict | None = None) -> list[dict]:
    """Best-effort per-fixture analysis rebuilt from stored signals (for the JSON feed)."""
    gates = _gates_by_fixture(gate_status or {})
    fa = {}
    for fid, meta in fixture_meta.items():
        fa[fid] = {**meta,
                   "bookable": _is_bookable(meta["league"], meta.get("country", "")),
                   "poisson": [], "mismatch": [], "bayesian": [], "dixon_coles": [],
                   "gates": gates.get(fid, []),
                   "strength": fixture_strength_all.get(fid)}
    for s in signals:
        entry = fa.get(s["fixture_id"])
        if not entry or s["model"] not in entry:
            continue
        entry[s["model"]].append({"market": s["unified_market"],
                                  "probability": s["probability"],
                                  **(s.get("detail") or {})})
    out = [v for v in fa.values()
           if v["poisson"] or v["mismatch"] or v["bayesian"] or v["dixon_coles"]]
    out.sort(key=lambda x: x.get("kickoff", ""))
    return out


def build_consensus_from_signals(signals: list[dict],
                                 target_odds: float = TARGET_ODDS,
                                 bankroll: float = 0, run_ds: str | None = None) -> dict:
    """Fan-in path: build the consensus pool/slips from already-computed model
    signals (read from raw.model_picks), with no model imports. Writes
    raw.consensus_pool when run_ds is given. See docs/engine_orchestration.md.
    """
    consensus = {}
    for s in signals:
        if s.get("probability") is None:
            continue
        key = (s["fixture_id"], s["unified_market"])
        c = consensus.setdefault(key, {"fixture_id": s["fixture_id"],
                                       "unified_market": s["unified_market"],
                                       "models": [], "confidences": [], "details": []})
        c["models"].append(s["model"])
        c["confidences"].append(s["probability"])
        if s.get("detail"):
            c["details"].append(s["detail"])

    fids = {s["fixture_id"] for s in signals}
    conn = get_conn()
    try:
        fixture_meta = fixture_meta_from_db(fids, conn)
        fixture_strength_all = _load_fixture_strength_profiles(conn)
    finally:
        conn.close()

    odds_map = _load_sportybet_odds()
    high_scoring_fids = _high_scoring_fixture_ids(fixture_meta.keys())
    ranked, gate_status = _score_pool(consensus, fixture_meta, fixture_strength_all,
                                      odds_map, high_scoring_fids)
    analysis = _analysis_from_signals(signals, fixture_meta, fixture_strength_all,
                                      gate_status)
    slips = _build_slips(ranked, target_odds, bankroll=bankroll)

    if run_ds:
        from engine.eval.picks_store import write_consensus_pool
        n = write_consensus_pool(run_ds, ranked)
        print(f"  wrote {n} rows to raw.consensus_pool (run_ds={run_ds})")

    return {"pool": ranked, "slips": slips, "fixture_analysis": analysis}


def run_consensus(days: int = 10, target_odds: float = TARGET_ODDS,
                  bankroll: float = 0) -> dict:
    """Run all four models and build consensus picks."""

    # ── 1. Poisson GLM ──────────────────────────────────────────────────
    print("=" * 60)
    print("  [1/4] POISSON GLM")
    print("=" * 60)
    poisson_picks = predict_upcoming(days=days)

    # ── 2. Mismatch Elo ─────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  [2/4] MISMATCH ELO")
    print("=" * 60)
    mismatch_picks, fixture_strength = predict_mismatch(days=days)

    # ── 3. Bayesian Streaks ──────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  [3/4] BAYESIAN STREAKS")
    print("=" * 60)
    from engine.eval.bayesian_streaks import _get_conn
    conn = _get_conn()
    bayesian_by_fixture = get_fixture_beliefs(conn, days=days)
    conn.close()
    print(f"  {len(bayesian_by_fixture)} fixtures with Bayesian beliefs")

    # ── 4. Dixon-Coles ───────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  [4/4] DIXON-COLES")
    print("=" * 60)
    dixon_coles_picks = predict_dixon_coles(days=days)
    print(f"  {len(dixon_coles_picks)} Dixon-Coles picks")

    # ── 5. Build unified pick pool ───────────────────────────────────────
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

    # From Dixon-Coles
    if not dixon_coles_picks.empty:
        for _, row in dixon_coles_picks.iterrows():
            fid = int(row["fixture_id"])
            if fid not in fixture_meta:
                fixture_meta[fid] = {
                    "fixture_id": fid,
                    "home": row["home"],
                    "away": row["away"],
                    "league": row["league"],
                    "kickoff": str(row["kickoff"]),
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

    conn = _get_conn()
    try:
        fixture_strength_all = _load_fixture_strength_profiles(conn)
    finally:
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

    # Collect normalised signals from every model (shared with run_model)
    _home_away = {fid: (m["home"], m["away"]) for fid, m in fixture_meta.items()}
    _signals = (poisson_signals(poisson_picks)
                + mismatch_signals(mismatch_picks)
                + bayesian_signals(bayesian_by_fixture, _home_away)
                + dixon_signals(dixon_coles_picks))
    for sig in _signals:
        _add_pick(sig["fixture_id"], sig["unified_market"], sig["model"],
                  sig["probability"], sig["detail"])

    # ── Load real SportyBet odds (populated by scrape_sportybet) ─────────
    odds_map = _load_sportybet_odds()
    if odds_map:
        print(f"  loaded real odds for {len(odds_map)} fixtures from raw.sportybet_odds")
    else:
        print("  no SportyBet odds in DB — run scrape_sportybet; falling back to est_odds")

    # ── Score and rank ──────────────────────────────────────────────────
    high_scoring_fids = _high_scoring_fixture_ids(fixture_meta.keys())
    ranked, gate_status = _score_pool(consensus, fixture_meta, fixture_strength_all,
                                      odds_map, high_scoring_fids)
    gates_by_fid = _gates_by_fixture(gate_status)

    # ── Per-fixture analysis (all raw model signals) ──────────────────
    fixture_analysis = {}
    for fid, meta in fixture_meta.items():
        fixture_analysis[fid] = {
            **meta,
            "bookable": _is_bookable(meta["league"], meta.get("country", "")),
            "gates": gates_by_fid.get(fid, []),
            "poisson": [],
            "mismatch": [],
            "bayesian": [],
            "dixon_coles": [],
        }

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
                if b["event"] == "BTTS":
                    continue
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

    if not dixon_coles_picks.empty:
        for _, row in dixon_coles_picks.iterrows():
            fid = int(row["fixture_id"])
            if fid in fixture_analysis:
                fixture_analysis[fid]["dixon_coles"].append({
                    "market": row["market"],
                    "probability": round(float(row["probability"]), 4),
                    "raw_probability": round(float(row["raw_probability"]), 4),
                    "tier": _tier(row["probability"]),
                    "lambda_home": row["lambda_home"],
                    "lambda_away": row["lambda_away"],
                })

    for fid, fa in fixture_analysis.items():
        fa["strength"] = fixture_strength_all.get(fid)

    analysis_list = [v for v in fixture_analysis.values()
                     if v["poisson"] or v["mismatch"] or v["bayesian"]
                     or v["dixon_coles"]]
    analysis_list.sort(key=lambda x: x.get("kickoff", ""))

    # ── Assemble slips ──────────────────────────────────────────────────
    slips = _build_slips(ranked, target_odds, bankroll=bankroll)
    return {"pool": ranked, "slips": slips, "fixture_analysis": analysis_list}


def _build_slips(ranked: list[dict], target_odds: float,
                 bankroll: float = 0, max_legs: int = MAX_LEGS,
                 min_ev: float = MIN_EV) -> list[dict]:
    """Build multiple short (3-5 leg) slips from the ranked pool.

    Only legs with real SportyBet odds and positive expected value
    (model_prob × odds >= min_ev) are bettable — no-odds legs stay in the
    pool/analysis but are excluded here. Diversifies across fixtures and
    assigns stakes by slip confidence tier.
    """
    ranked = [p for p in ranked
              if p.get("has_odds") and p.get("ev") is not None and p["ev"] >= min_ev]

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
            cum_odds *= pick["odds"]

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
        print(f"  {'#':>2s}  {'Models':>6s}  {'Conf':>5s}  {'Odds':>5s}  {'EV':>5s}  {'Kickoff':>18s}  {'Match':<40s}  {'Selection':<28s}  League")
        print(f"  {'─'*2}  {'─'*6}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*18}  {'─'*40}  {'─'*28}  {'─'*20}")

        for i, sel in enumerate(sels, 1):
            models_str = "+".join(m[0].upper() for m in sel["models"])
            try:
                dt = pd.Timestamp(sel["kickoff"]).strftime("%a %b %d %H:%M")
            except Exception:
                dt = sel["kickoff"][:16]
            ev_str = f"{sel['ev']:.2f}" if sel.get("ev") is not None else "  —"
            print(f"  {i:>2d}  {models_str:>6s}  {sel['avg_confidence']:>4.0%}  {sel['odds']:>5.2f}  {ev_str:>5s}  "
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
            "real_odds": p.get("real_odds"),
            "odds": p.get("odds"),
            "has_odds": p.get("has_odds", False),
            "ev": p.get("ev"),
            "country": p.get("country", ""),
            "strength_gate": p.get("strength_gate"),
            "strength_rank": p.get("strength_rank"),
            "market_tier": p.get("market_tier"),
        })

    output = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "models": ["poisson_glm", "mismatch_elo", "bayesian_streaks",
                   "dixon_coles"],
        "thresholds": {
            "solo": SOLO_THRESHOLD,
            "duo": DUO_THRESHOLD,
            "trio": TRIO_THRESHOLD,
            "over15_dc_poisson": OVER15_DC_POISSON_THRESHOLD,
            "under35_strength": {
                "max_avg_attack_pct": UNDER35_MAX_AVG_ATTACK_PCT,
                "max_single_attack_pct": UNDER35_MAX_SINGLE_ATTACK_PCT,
                "min_avg_defense_pct": UNDER35_MIN_AVG_DEFENSE_PCT,
                "min_single_defense_pct": UNDER35_MIN_SINGLE_DEFENSE_PCT,
            },
            "under35_tiers": {
                "A": UNDER35_TIER_A_THRESHOLD,
                "B": UNDER35_TIER_B_THRESHOLD,
                "C": UNDER35_TIER_C_THRESHOLD,
            },
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
                "real_odds": sel.get("real_odds"),
                "odds": sel.get("odds"),
                "ev": sel.get("ev"),
                "strength_gate": sel.get("strength_gate"),
                "strength_rank": sel.get("strength_rank"),
                "market_tier": sel.get("market_tier"),
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
    parser.add_argument("--from-picks", action="store_true",
                        help="Fan-in mode: build from raw.model_picks (set by run_model) "
                             "instead of running the 4 models in-process")
    parser.add_argument("--run-ds", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        help="Prediction run date (used with --from-picks)")
    args = parser.parse_args()

    if args.from_picks:
        from engine.eval.picks_store import read_model_picks
        signals = read_model_picks(args.run_ds)
        print(f"Fan-in: {len(signals)} signals from raw.model_picks (run_ds={args.run_ds})")
        result = build_consensus_from_signals(
            signals, target_odds=args.target_odds, bankroll=args.bankroll,
            run_ds=args.run_ds)
    else:
        result = run_consensus(days=args.days, target_odds=args.target_odds,
                               bankroll=args.bankroll)
    print_slips(result)
    if result["slips"]:
        save_slips(result)
