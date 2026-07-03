"""
Walk-forward calibration backtest — no data leakage.

Splits 6 months of history into 2-week test folds. For each fold,
trains models ONLY on data before that fold's start date, then predicts
that fold's completed fixtures. De-duplicates to one prediction per
(fixture, market) and compares to actual outcomes.

Usage:
    python -m engine.eval.calibration_wf
    python -m engine.eval.calibration_wf --months 6 --fold-days 14
"""

from __future__ import annotations

import argparse
import json
import warnings
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

from engine.eval.bayesian_streaks import (
    BETTING_EVENTS,
    HALF_LIFE,
    MIN_POSTERIOR_INTERESTING,
    PRIOR_STRENGTH,
    compute_beliefs,
    compute_league_base_rates,
)
from engine.eval.calibration_maps import MAPS_PATH, apply_points, fit_pava_map
from etl.src.logger import get_logger

logger = get_logger(__name__)

EVENTS_TABLE = "raw.fact_head_to_head_events"
H2H_TABLE = "raw.fact_head_to_head"
ELO_TABLE = "elo.team_elo_history"

# Folds record RAW model probabilities down to these floors (below the live
# betting gates) so the calibration fit has support around the thresholds.
# Live gates are applied later, at evaluation time only.
RECORD_FLOOR_BAYESIAN = 0.60
RECORD_FLOOR_POISSON = 0.55
RECORD_FLOOR_MISMATCH = 0.60

# Fraction of folds used to FIT calibration maps; the rest are held out
# for evaluation so the report never grades a map on the data that fit it.
CAL_FRAC_DEFAULT = 0.4


def _get_conn():
    return psycopg2.connect(
        dbname="ballistics",
        user="ballistics_user",
        password="ronaldo",
        host="localhost",
        port=5432,
    )


def _actual_outcome(row: pd.Series, market: str) -> bool | None:
    hg = row.get("goals_home_ft")
    ag = row.get("goals_away_ft")
    if hg is None or ag is None:
        return None
    hg, ag = int(hg), int(ag)
    total = hg + ag

    if "Match Over" in market:
        return total > float(market.split()[-1])
    if "Match Under" in market:
        return total < float(market.split()[-1])
    if "Over 0.5" in market:
        team = market.replace(" Over 0.5", "").strip()
        if team == row.get("home_team_name", ""):
            return hg >= 1
        if team == row.get("away_team_name", ""):
            return ag >= 1
        return None
    if "Over 1.5" in market and "Match" not in market:
        team = market.replace(" Over 1.5", "").strip()
        if team == row.get("home_team_name", ""):
            return hg >= 2
        if team == row.get("away_team_name", ""):
            return ag >= 2
        return None
    if "Win" in market:
        team = market.replace(" Win", "").strip()
        if team == row.get("home_team_name", ""):
            return hg > ag
        if team == row.get("away_team_name", ""):
            return ag > hg
        return None
    if "BTTS" in market:
        return hg >= 1 and ag >= 1
    return None


# ── Data loaders with date cutoff ──────────────────────────────────────

def load_all_fixtures(conn, months: int = 6) -> pd.DataFrame:
    cutoff = datetime.now(timezone.utc) - timedelta(days=months * 30)
    return pd.read_sql("""
        SELECT fixture_id, home_team_id, home_team_name,
               away_team_id, away_team_name,
               home_team_fulltime_goal AS goals_home_ft,
               away_team_fulltime_goal AS goals_away_ft,
               kickoff_utc, api_league_id, fixture_status
        FROM raw.raw_fixtures
        WHERE fixture_status = 'FT'
          AND kickoff_utc >= %(cutoff)s
        ORDER BY kickoff_utc
    """, conn, params={"cutoff": cutoff})


def _load_event_history_before(conn, cutoff_dt, context: str) -> pd.DataFrame:
    """Load Bayesian event history for fixtures BEFORE cutoff only."""
    return pd.read_sql(f"""
        SELECT e.league_name AS league, e.team_id, e.team_name,
               e.event_name, e.event_flag, e.kickoff_utc, e.fixture_id
        FROM {EVENTS_TABLE} e
        WHERE e.h2h_context_scope = %(context)s
          AND e.kickoff_utc < %(cutoff)s
        ORDER BY e.team_id, e.event_name, e.kickoff_utc
    """, conn, params={"context": context, "cutoff": cutoff_dt})


def _load_base_rates_before(conn, cutoff_dt, context: str) -> pd.DataFrame:
    """Compute league base rates from events BEFORE cutoff only."""
    return pd.read_sql(f"""
        SELECT league_name AS league, event_name,
               AVG(event_flag::float) AS base_rate, COUNT(*) AS n
        FROM {EVENTS_TABLE}
        WHERE h2h_context_scope = %(context)s
          AND kickoff_utc < %(cutoff)s
        GROUP BY league_name, event_name
    """, conn, params={"context": context, "cutoff": cutoff_dt})


def _load_id_maps(conn) -> tuple[dict, dict]:
    """Load raw_fixtures team ID → fact_head_to_head team ID mapping."""
    fh_id_map_df = pd.read_sql("""
        SELECT DISTINCT r.home_team_id AS rf_id, f.team_id AS fh_id
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head f ON f.fixture_id = r.fixture_id AND f.is_home
        UNION
        SELECT DISTINCT r.away_team_id, f.opponent_id
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head f ON f.fixture_id = r.fixture_id AND f.is_home
    """, conn)
    id_map = dict(zip(fh_id_map_df["rf_id"], fh_id_map_df["fh_id"]))

    league_map_df = pd.read_sql("""
        SELECT DISTINCT r.api_league_id, f.league_name AS league
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head f ON f.fixture_id = r.fixture_id AND f.is_home
    """, conn)
    league_map = dict(zip(league_map_df["api_league_id"], league_map_df["league"]))
    return id_map, league_map


def _load_historical_h2h_before(conn, cutoff_dt) -> pd.DataFrame:
    """Load fact_head_to_head with Elo for played fixtures BEFORE cutoff."""
    return pd.read_sql(f"""
        SELECT f.fixture_id, f.kickoff_utc, f.season, f.league_name AS league,
               f.team_id AS home_team_id, f.opponent_id AS away_team_id,
               f.team_name AS home_team, f.opponent_name AS away_team,
               f.goals_for AS home_goals, f.goals_against AS away_goals,
               COALESCE(eh.rating_before_flat, eh.rating_before) AS home_elo,
               COALESCE(ea.rating_before_flat, ea.rating_before) AS away_elo
        FROM {H2H_TABLE} f
        JOIN {ELO_TABLE} eh ON eh.team_id = f.team_id AND eh.fixture_id = f.fixture_id
        JOIN {ELO_TABLE} ea ON ea.team_id = f.opponent_id AND ea.fixture_id = f.fixture_id
        WHERE f.is_played AND f.is_home
          AND f.kickoff_utc < %(cutoff)s
        ORDER BY f.kickoff_utc
    """, conn, params={"cutoff": cutoff_dt})


def _load_elo_before(conn, cutoff_dt, team_ids: list, id_map: dict) -> dict:
    """Get latest Elo for each team using only data before cutoff."""
    mapped = [id_map.get(t, t) for t in team_ids]
    if not mapped:
        return {}
    placeholders = ",".join(["%s"] * len(mapped))
    params = mapped + [cutoff_dt]
    df = pd.read_sql(f"""
        SELECT DISTINCT ON (team_id)
            team_id, COALESCE(rating_after_flat, rating_after) AS elo
        FROM {ELO_TABLE}
        WHERE team_id IN ({placeholders})
          AND kickoff_utc < %s
        ORDER BY team_id, kickoff_utc DESC
    """, conn, params=params)

    elo_map = dict(zip(df["team_id"], df["elo"]))
    result = {}
    for rf_id in team_ids:
        fh_id = id_map.get(rf_id, rf_id)
        if fh_id in elo_map:
            result[rf_id] = float(elo_map[fh_id])
    return result


def _build_mismatch_hit_rates_before(conn, cutoff_dt) -> pd.DataFrame:
    """Build mismatch hit rates from fixtures BEFORE cutoff only."""
    from engine.eval.predict_mismatch import GAP_BINS, MIN_SAMPLE

    sql = f"""
        WITH matches AS (
            SELECT f.fixture_id, f.league_name,
                   f.goals_for + f.goals_against AS total,
                   ABS(COALESCE(eh.rating_before_flat, eh.rating_before)
                     - COALESCE(ea.rating_before_flat, ea.rating_before)) AS gap,
                   CASE WHEN COALESCE(eh.rating_before_flat, eh.rating_before)
                           > COALESCE(ea.rating_before_flat, ea.rating_before)
                        THEN f.goals_for ELSE f.goals_against END AS fav_goals,
                   CASE WHEN COALESCE(eh.rating_before_flat, eh.rating_before)
                           > COALESCE(ea.rating_before_flat, ea.rating_before)
                        THEN f.goals_against ELSE f.goals_for END AS dog_goals
            FROM {H2H_TABLE} f
            JOIN {ELO_TABLE} eh ON eh.team_id = f.team_id AND eh.fixture_id = f.fixture_id
            JOIN {ELO_TABLE} ea ON ea.team_id = f.opponent_id AND ea.fixture_id = f.fixture_id
            WHERE f.is_played AND f.is_home
              AND f.kickoff_utc < %(cutoff)s
        )
        SELECT league_name, gap, fav_goals, dog_goals, total
        FROM matches WHERE gap >= 100
    """
    df = pd.read_sql(sql, conn, params={"cutoff": cutoff_dt})
    if df.empty:
        return pd.DataFrame()

    rows = []
    for league, grp in df.groupby("league_name"):
        for lo, hi, label in GAP_BINS:
            bin_df = grp[(grp["gap"] >= lo) & (grp["gap"] < hi)]
            n = len(bin_df)
            if n < MIN_SAMPLE:
                continue
            rows.append({
                "league": league, "gap_bin": label, "n": n,
                "fav_scores_pct": (bin_df["fav_goals"] >= 1).mean(),
                "fav_over15_pct": (bin_df["fav_goals"] >= 2).mean(),
                "over15_pct": (bin_df["total"] > 1.5).mean(),
                "over25_pct": (bin_df["total"] > 2.5).mean(),
                "fav_win_pct": (bin_df["fav_goals"] > bin_df["dog_goals"]).mean(),
                "draw_pct": (bin_df["fav_goals"] == bin_df["dog_goals"]).mean(),
                "avg_fav_goals": bin_df["fav_goals"].mean(),
                "avg_total": bin_df["total"].mean(),
            })

    for lo, hi, label in GAP_BINS:
        bin_df = df[(df["gap"] >= lo) & (df["gap"] < hi)]
        n = len(bin_df)
        if n < MIN_SAMPLE:
            continue
        rows.append({
            "league": "__global__", "gap_bin": label, "n": n,
            "fav_scores_pct": (bin_df["fav_goals"] >= 1).mean(),
            "fav_over15_pct": (bin_df["fav_goals"] >= 2).mean(),
            "over15_pct": (bin_df["total"] > 1.5).mean(),
            "over25_pct": (bin_df["total"] > 2.5).mean(),
            "fav_win_pct": (bin_df["fav_goals"] > bin_df["dog_goals"]).mean(),
            "draw_pct": (bin_df["fav_goals"] == bin_df["dog_goals"]).mean(),
            "avg_fav_goals": bin_df["fav_goals"].mean(),
            "avg_total": bin_df["total"].mean(),
        })

    return pd.DataFrame(rows)


# ── Per-fold model runners ─────────────────────────────────────────────

def _bayesian_fold(conn, test_fixtures: pd.DataFrame, cutoff_dt,
                   id_map: dict) -> list[dict]:
    """Bayesian predictions using only events before cutoff."""
    results = []
    all_rf_ids = set(test_fixtures["home_team_id"].tolist() +
                     test_fixtures["away_team_id"].tolist())
    all_fh_ids = {id_map.get(rf, rf) for rf in all_rf_ids}

    for ctx in ("home", "away"):
        event_df = _load_event_history_before(conn, cutoff_dt, ctx)
        event_df = event_df[
            event_df["team_id"].isin(all_fh_ids)
            & event_df["event_name"].isin(BETTING_EVENTS)
        ]
        if event_df.empty:
            continue
        base_rates = _load_base_rates_before(conn, cutoff_dt, ctx)
        beliefs = compute_beliefs(event_df, base_rates)

        belief_by_team = defaultdict(list)
        for b in beliefs:
            b.context = ctx
            belief_by_team[b.team_id].append(b)

        for _, fix in test_fixtures.iterrows():
            fid = int(fix["fixture_id"])
            if ctx == "home":
                target_rf = fix["home_team_id"]
                team_name = fix["home_team_name"]
                opp_name = fix["away_team_name"]
            else:
                target_rf = fix["away_team_id"]
                team_name = fix["away_team_name"]
                opp_name = fix["home_team_name"]

            target_fh = id_map.get(target_rf, target_rf)
            for b in belief_by_team.get(target_fh, []):
                if b.posterior < RECORD_FLOOR_BAYESIAN:
                    continue
                market = None
                if b.event_name == "Over_1_5":
                    market = "Match Over 1.5"
                elif b.event_name == "Over_2_5":
                    market = "Match Over 2.5"
                elif b.event_name == "Under_2_5":
                    market = "Match Under 2.5"
                elif b.event_name == "Under_3_5":
                    market = "Match Under 3.5"
                elif b.event_name == "score_1goal":
                    market = f"{team_name} Over 0.5"
                elif b.event_name == "score_2goals":
                    market = f"{team_name} Over 1.5"
                elif b.event_name == "concede_1":
                    market = f"{opp_name} Over 0.5"
                elif b.event_name == "BTTS":
                    market = "BTTS Yes"

                if market:
                    outcome = _actual_outcome(fix, market)
                    if outcome is not None:
                        results.append({
                            "fixture_id": fid,
                            "model": "bayesian",
                            "market": market,
                            "predicted_prob_raw": round(float(b.posterior), 4),
                            "actual": outcome,
                        })
    return results


def _poisson_fold(conn, test_fixtures: pd.DataFrame, cutoff_dt,
                  id_map: dict, league_map: dict) -> list[dict]:
    """Poisson predictions trained only on fixtures before cutoff.

    Mirrors live predict_upcoming exactly: per-cluster Poisson on OVER_COLS
    for overs, a separate global Poisson on UNDER_COLS for unders, and the
    live lines [2.5, 3.5]. Records RAW probabilities — calibration is
    fitted/applied downstream, never inside the fold.
    """
    from engine.eval.predict_upcoming import (
        prepare_features, compute_form_features, compute_strength_features,
        compute_rest_days, assign_clusters_seasonal, _fit_poisson_lambda,
        _predict_poisson_over, OVER_COLS, UNDER_COLS, MIN_TRAIN_PER_CLUSTER,
    )
    from engine.eval.cluster_backtest import ELO_GAP_SENSITIVITY, _MEDIAN_SENSITIVITY

    results = []
    hist_raw = _load_historical_h2h_before(conn, cutoff_dt)
    if len(hist_raw) < 100:
        return results
    hist = compute_form_features(prepare_features(hist_raw))

    test_df = test_fixtures.copy()
    test_df["league"] = test_df["api_league_id"].map(league_map)
    test_df = test_df[test_df["league"].notna()].copy()
    if test_df.empty:
        return results

    all_teams = set(test_df["home_team_id"].tolist() +
                    test_df["away_team_id"].tolist())
    elo_map = _load_elo_before(conn, cutoff_dt, list(all_teams), id_map)

    test_df["home_elo"] = test_df["home_team_id"].map(elo_map).fillna(1500.0)
    test_df["away_elo"] = test_df["away_team_id"].map(elo_map).fillna(1500.0)
    test_df["abs_gap"] = (test_df["home_elo"] - test_df["away_elo"]).abs()
    test_df["max_elo"] = test_df[["home_elo", "away_elo"]].max(axis=1)
    test_df["min_elo"] = test_df[["home_elo", "away_elo"]].min(axis=1)
    test_df["elo_sum"] = test_df["home_elo"] + test_df["away_elo"]
    test_df["home_elo_gap"] = test_df["home_elo"] - test_df["away_elo"]
    test_df["kickoff_utc"] = pd.to_datetime(test_df["kickoff_utc"], utc=True)

    sensitivity = test_df["league"].map(ELO_GAP_SENSITIVITY).fillna(_MEDIAN_SENSITIVITY)
    test_df["elo_gap_scaled"] = test_df["abs_gap"] * sensitivity

    test_s = compute_strength_features(hist, test_df)
    test_s = compute_rest_days(test_s)
    train_s = compute_strength_features(hist, hist)
    train_s = compute_rest_days(train_s)
    train_s["cluster"] = assign_clusters_seasonal(train_s, train_s)
    test_s["cluster"] = assign_clusters_seasonal(train_s, test_s)

    for line in [2.5, 3.5]:
        # ── OVER: per-cluster Poisson on OVER_COLS (as live) ──
        clusters = sorted(train_s["cluster"].unique())
        over_probs = np.full(len(test_s), np.nan)

        for cl in clusters:
            tr_cl = train_s[train_s["cluster"] == cl]
            te_cl = test_s[test_s["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(tr_cl["total_goals"].values,
                                             tr_cl[OVER_COLS].values)
            idx = te_cl.index
            probs = _predict_poisson_over(params, te_cl[OVER_COLS].values, line)
            over_probs[test_s.index.get_indexer(idx)] = probs

        nan_mask = np.isnan(over_probs)
        if nan_mask.any():
            over_probs[nan_mask] = (train_s["total_goals"] > line).mean()

        # ── UNDER: separate global Poisson on UNDER_COLS (as live) ──
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            params_u = _fit_poisson_lambda(train_s["total_goals"].values,
                                           train_s[UNDER_COLS].values)
        under_probs = 1.0 - _predict_poisson_over(
            params_u, test_s[UNDER_COLS].values, line)

        for i, (_, row) in enumerate(test_s.iterrows()):
            fid = int(row["fixture_id"])
            fix_row = test_fixtures[test_fixtures["fixture_id"] == fid]
            if fix_row.empty:
                continue
            fix_row = fix_row.iloc[0]

            for mkt, prob in [(f"Match Over {line}", float(over_probs[i])),
                              (f"Match Under {line}", float(under_probs[i]))]:
                if prob < RECORD_FLOOR_POISSON:
                    continue
                outcome = _actual_outcome(fix_row, mkt)
                if outcome is not None:
                    results.append({
                        "fixture_id": fid, "model": "poisson",
                        "market": mkt, "predicted_prob_raw": round(prob, 4),
                        "actual": outcome,
                    })
    return results


def _mismatch_fold(conn, test_fixtures: pd.DataFrame, cutoff_dt,
                   id_map: dict, league_map: dict) -> list[dict]:
    """Mismatch predictions using hit rates computed only from pre-cutoff data."""
    from engine.eval.predict_mismatch import (
        _get_gap_bin, _load_h2h_records, _load_team_strength,
        _h2h_adjustment, _lookup_hit_rate, _strength_adjustment,
        MIN_CONFIDENCE, MIN_SAMPLE,
    )

    results = []
    hit_rates = _build_mismatch_hit_rates_before(conn, cutoff_dt)
    if hit_rates.empty:
        return results

    test_df = test_fixtures.copy()
    test_df["league"] = test_df["api_league_id"].map(league_map)
    test_df = test_df[test_df["league"].notna()].copy()
    if test_df.empty:
        return results

    all_teams = set(test_df["home_team_id"].tolist() +
                    test_df["away_team_id"].tolist())
    elo_map = _load_elo_before(conn, cutoff_dt, list(all_teams), id_map)

    fixture_info = []
    for _, row in test_df.iterrows():
        h_elo = elo_map.get(row["home_team_id"], 1500.0)
        a_elo = elo_map.get(row["away_team_id"], 1500.0)
        gap = abs(h_elo - a_elo)
        if gap < 100:
            continue
        bin_info = _get_gap_bin(gap)
        if bin_info is None:
            continue
        fav_is_home = h_elo >= a_elo
        fav_rf = row["home_team_id"] if fav_is_home else row["away_team_id"]
        dog_rf = row["away_team_id"] if fav_is_home else row["home_team_id"]
        fixture_info.append((row, h_elo, a_elo, gap, bin_info,
                             fav_is_home, fav_rf, dog_rf))

    if not fixture_info:
        return results

    matchups = [(f[6], f[7]) for f in fixture_info]
    h2h_map = _load_h2h_records(conn, id_map, matchups, cutoff=cutoff_dt)
    strength_map = _load_team_strength(conn, id_map, cutoff=cutoff_dt)

    for row, h_elo, a_elo, gap, bin_info, fav_is_home, fav_rf, dog_rf in fixture_info:
        gap_bin, _, _ = bin_info
        fav_name = row["home_team_name"] if fav_is_home else row["away_team_name"]
        dog_name = row["away_team_name"] if fav_is_home else row["home_team_name"]

        h2h_w, h2h_d, h2h_l = h2h_map.get((fav_rf, dog_rf), (0, 0, 0))
        h2h_boost, _ = _h2h_adjustment(h2h_w, h2h_d, h2h_l)
        fav_strength = strength_map.get(fav_rf)
        dog_strength = strength_map.get(dog_rf)

        fix_row = test_fixtures[test_fixtures["fixture_id"] == row["fixture_id"]]
        if fix_row.empty:
            continue
        fix_row = fix_row.iloc[0]

        for mkt in ["fav_to_score", "fav_over_1.5", "over_1.5", "over_2.5"]:
            conf, n = _lookup_hit_rate(hit_rates, row.get("league", ""), gap_bin, mkt)
            str_adj = _strength_adjustment(mkt, fav_strength, dog_strength)
            adjusted = max(min(conf + str_adj, 0.99), 0.0)
            if adjusted < RECORD_FLOOR_MISMATCH or n < MIN_SAMPLE:
                continue
            if mkt == "fav_to_score":
                market = f"{fav_name} Over 0.5"
            elif mkt == "fav_over_1.5":
                market = f"{fav_name} Over 1.5"
            elif mkt == "over_1.5":
                market = "Match Over 1.5"
            elif mkt == "over_2.5":
                market = "Match Over 2.5"
            else:
                continue

            outcome = _actual_outcome(fix_row, market)
            if outcome is not None:
                results.append({
                    "fixture_id": int(row["fixture_id"]),
                    "model": "mismatch", "market": market,
                    "predicted_prob_raw": round(adjusted, 4),
                    "actual": outcome,
                })

        win_conf, win_n = _lookup_hit_rate(
            hit_rates, row.get("league", ""), gap_bin, "fav_win")
        adjusted_win = min(win_conf + h2h_boost, 0.95)
        if adjusted_win >= RECORD_FLOOR_MISMATCH and win_n >= MIN_SAMPLE:
            market = f"{fav_name} Win"
            outcome = _actual_outcome(fix_row, market)
            if outcome is not None:
                results.append({
                    "fixture_id": int(row["fixture_id"]),
                    "model": "mismatch", "market": market,
                    "predicted_prob_raw": round(adjusted_win, 4),
                    "actual": outcome,
                })

    return results


# ── De-duplication ─────────────────────────────────────────────────────

def deduplicate_predictions(preds: list[dict]) -> list[dict]:
    """One prediction per (fixture_id, market). Average confidence across models."""
    grouped = defaultdict(list)
    for p in preds:
        grouped[(p["fixture_id"], p["market"])].append(p)

    deduped = []
    for (fid, market), group in grouped.items():
        avg_prob = np.mean([p["predicted_prob"] for p in group])
        models = sorted(set(p["model"] for p in group))
        deduped.append({
            "fixture_id": fid,
            "market": market,
            "predicted_prob": round(float(avg_prob), 4),
            "actual": group[0]["actual"],
            "n_models": len(models),
            "models": ",".join(models),
            "home": group[0].get("home", ""),
            "away": group[0].get("away", ""),
            "score": group[0].get("score", ""),
            "kickoff": group[0].get("kickoff", ""),
        })
    return deduped


# ── Calibration metrics ────────────────────────────────────────────────

def compute_calibration(predictions: list[dict], n_bins: int = 10) -> dict:
    df = pd.DataFrame(predictions)
    if df.empty:
        return {"n": 0}

    df["actual_int"] = df["actual"].astype(int)
    brier = float(((df["predicted_prob"] - df["actual_int"]) ** 2).mean())

    bins = np.linspace(0.5, 1.0, n_bins + 1)
    bin_data = []
    for i in range(len(bins) - 1):
        lo, hi = bins[i], bins[i + 1]
        mask = (df["predicted_prob"] >= lo) & (df["predicted_prob"] < hi)
        subset = df[mask]
        if len(subset) == 0:
            continue
        bin_data.append({
            "bin_lo": round(lo, 3), "bin_hi": round(hi, 3),
            "bin_mid": round((lo + hi) / 2, 3), "n": len(subset),
            "predicted_avg": round(float(subset["predicted_prob"].mean()), 4),
            "actual_rate": round(float(subset["actual_int"].mean()), 4),
            "gap": round(float(subset["predicted_prob"].mean() -
                               subset["actual_int"].mean()), 4),
        })

    per_model = {}
    if "models" in df.columns:
        # Explode to per-model stats
        all_models = set()
        for m in df["models"]:
            all_models.update(m.split(","))
        for model in sorted(all_models):
            grp = df[df["models"].str.contains(model)]
            if len(grp) == 0:
                continue
            model_brier = float(((grp["predicted_prob"] - grp["actual_int"]) ** 2).mean())
            per_model[model] = {
                "n": len(grp),
                "brier": round(model_brier, 4),
                "avg_predicted": round(float(grp["predicted_prob"].mean()), 4),
                "actual_hit_rate": round(float(grp["actual_int"].mean()), 4),
                "overconfidence": round(float(grp["predicted_prob"].mean() -
                                              grp["actual_int"].mean()), 4),
            }

    # Consensus tiers
    consensus_stats = {}
    if "n_models" in df.columns:
        for label, mask in [("trio", df["n_models"] >= 3),
                            ("duo", df["n_models"] == 2),
                            ("solo", df["n_models"] == 1)]:
            sub = df[mask]
            if len(sub) == 0:
                continue
            consensus_stats[label] = {
                "n": len(sub),
                "avg_predicted": round(float(sub["predicted_prob"].mean()), 4),
                "actual_hit_rate": round(float(sub["actual_int"].mean()), 4),
                "brier": round(float(((sub["predicted_prob"] -
                                       sub["actual_int"]) ** 2).mean()), 4),
            }

    # Market type breakdown
    def _market_type(m):
        if "Under" in m:
            return "under"
        if "Over 0.5" in m:
            return "team_scores"
        if "Over" in m:
            return "over_match"
        if "Win" in m:
            return "win"
        return "other"

    per_market = {}
    df["mtype"] = df["market"].apply(_market_type)
    for mt, grp in df.groupby("mtype"):
        per_market[mt] = {
            "n": len(grp),
            "avg_predicted": round(float(grp["predicted_prob"].mean()), 4),
            "actual_hit_rate": round(float(grp["actual_int"].mean()), 4),
            "brier": round(float(((grp["predicted_prob"] -
                                   grp["actual_int"]) ** 2).mean()), 4),
        }

    return {
        "n": len(df),
        "unique_fixtures": int(df["fixture_id"].nunique()),
        "brier_score": round(brier, 4),
        "avg_predicted": round(float(df["predicted_prob"].mean()), 4),
        "overall_hit_rate": round(float(df["actual_int"].mean()), 4),
        "overconfidence": round(float(df["predicted_prob"].mean() -
                                      df["actual_int"].mean()), 4),
        "calibration_bins": bin_data,
        "per_model": per_model,
        "consensus": consensus_stats,
        "per_market": per_market,
    }


# ── Base rate comparison ───────────────────────────────────────────────

def compute_base_rates(fixtures: pd.DataFrame) -> dict:
    n = len(fixtures)
    if n == 0:
        return {}
    hg = fixtures["goals_home_ft"].astype(int)
    ag = fixtures["goals_away_ft"].astype(int)
    total = hg + ag
    return {
        "n_fixtures": n,
        "home_scores": round(float((hg >= 1).mean()), 4),
        "away_scores": round(float((ag >= 1).mean()), 4),
        "over_1.5": round(float((total > 1.5).mean()), 4),
        "over_2.5": round(float((total > 2.5).mean()), 4),
        "under_3.5": round(float((total < 3.5).mean()), 4),
    }


# ── Main walk-forward runner ──────────────────────────────────────────

def run_walk_forward(months: int = 6, fold_days: int = 14,
                     cal_frac: float = CAL_FRAC_DEFAULT) -> dict:
    conn = _get_conn()
    try:
        print("=" * 60)
        print("  WALK-FORWARD CALIBRATION (no data leakage)")
        print(f"  Period: last {months} months | fold size: {fold_days} days")
        print(f"  Calibration maps fitted on first {cal_frac:.0%} of folds,")
        print(f"  evaluated on the remainder only")
        print("=" * 60)

        print("\nLoading all completed fixtures...")
        all_fixtures = load_all_fixtures(conn, months=months)
        all_fixtures["kickoff_utc"] = pd.to_datetime(
            all_fixtures["kickoff_utc"], utc=True)
        print(f"  {len(all_fixtures)} fixtures loaded")

        print("Loading ID maps...")
        id_map, league_map = _load_id_maps(conn)

        # Build folds: first 50% of the period is min training data,
        # then test in fold_days blocks
        min_date = all_fixtures["kickoff_utc"].min()
        max_date = all_fixtures["kickoff_utc"].max()
        total_days = (max_date - min_date).days

        # Start testing after at least 60 days of training data
        min_train_days = 60
        test_start = min_date + timedelta(days=min_train_days)

        folds = []
        current = test_start
        while current < max_date:
            fold_end = min(current + timedelta(days=fold_days), max_date)
            folds.append((current, fold_end))
            current = fold_end

        print(f"  {len(folds)} test folds (training cutoff moves forward)")
        print(f"  Training starts: {min_date.date()}")
        print(f"  Test starts:     {test_start.date()}")
        print(f"  Test ends:       {max_date.date()}")

        all_predictions = []
        all_test_fixtures = pd.DataFrame()

        for i, (fold_start, fold_end) in enumerate(folds):
            test_mask = ((all_fixtures["kickoff_utc"] >= fold_start) &
                         (all_fixtures["kickoff_utc"] < fold_end))
            test_fix = all_fixtures[test_mask].copy()
            if test_fix.empty:
                continue

            cutoff_dt = fold_start
            n_train = len(all_fixtures[all_fixtures["kickoff_utc"] < cutoff_dt])

            print(f"\n  Fold {i+1}/{len(folds)}: "
                  f"{fold_start.date()} → {fold_end.date()} | "
                  f"{len(test_fix)} test fixtures | "
                  f"{n_train} training fixtures")

            fold_preds = []

            # Bayesian
            try:
                bp = _bayesian_fold(conn, test_fix, cutoff_dt, id_map)
                fold_preds.extend(bp)
                print(f"    Bayesian: {len(bp)} raw predictions")
            except Exception as e:
                print(f"    Bayesian failed: {e}")

            # Poisson
            try:
                pp = _poisson_fold(conn, test_fix, cutoff_dt, id_map, league_map)
                fold_preds.extend(pp)
                print(f"    Poisson:  {len(pp)} raw predictions")
            except Exception as e:
                print(f"    Poisson failed: {e}")

            # Mismatch
            try:
                mp = _mismatch_fold(conn, test_fix, cutoff_dt, id_map, league_map)
                fold_preds.extend(mp)
                print(f"    Mismatch: {len(mp)} raw predictions")
            except Exception as e:
                print(f"    Mismatch failed: {e}")

            # Attach fixture info
            fix_lookup = test_fix.set_index("fixture_id").to_dict("index")
            for pred in fold_preds:
                finfo = fix_lookup.get(pred["fixture_id"], {})
                pred["home"] = finfo.get("home_team_name", "")
                pred["away"] = finfo.get("away_team_name", "")
                hg = int(finfo.get("goals_home_ft", 0))
                ag = int(finfo.get("goals_away_ft", 0))
                pred["score"] = f"{hg}-{ag}"
                pred["kickoff"] = str(finfo.get("kickoff_utc", ""))[:16]
                pred["fold"] = i + 1

            test_fix["fold"] = i + 1
            all_predictions.extend(fold_preds)
            all_test_fixtures = pd.concat([all_test_fixtures, test_fix],
                                          ignore_index=True)

        print(f"\n{'─' * 60}")
        print(f"  Total raw predictions: {len(all_predictions)}")

        # ── Fit/eval fold split — maps are never graded on their own data ──
        n_folds = len(folds)
        n_fit = min(max(1, int(round(n_folds * cal_frac))), n_folds - 1)
        fit_preds = [p for p in all_predictions if p["fold"] <= n_fit]
        eval_preds = [p for p in all_predictions if p["fold"] > n_fit]
        print(f"  Calibration-fit folds 1-{n_fit}: {len(fit_preds)} predictions")
        print(f"  Evaluation folds {n_fit + 1}-{n_folds}: "
              f"{len(eval_preds)} predictions")

        print("\nFitting calibration maps (fit folds only)...")
        fit_maps = {}
        for model in ("bayesian", "poisson", "mismatch"):
            mp = [p for p in fit_preds if p["model"] == model]
            points = fit_pava_map([p["predicted_prob_raw"] for p in mp],
                                  [float(p["actual"]) for p in mp])
            fit_maps[model] = points
            print(f"  {model:<10s} {len(mp):>6d} preds → "
                  f"{f'{len(points)} map points' if points else 'insufficient data (identity)'}")

        # ── Apply maps + LIVE gates to eval-fold predictions ──
        # Gates mirror production: bayesian raw >= MIN_POSTERIOR_INTERESTING,
        # poisson calibrated >= CALIBRATED_THRESHOLD, mismatch raw >= MIN_CONFIDENCE.
        from engine.eval.predict_upcoming import CALIBRATED_THRESHOLD
        from engine.eval.predict_mismatch import MIN_CONFIDENCE as MM_MIN_CONFIDENCE

        gated = []
        for p in eval_preds:
            raw = p["predicted_prob_raw"]
            points = fit_maps.get(p["model"]) or []
            cal = apply_points(points, raw) if points else raw
            if p["model"] == "bayesian" and raw < MIN_POSTERIOR_INTERESTING:
                continue
            if p["model"] == "poisson" and cal < CALIBRATED_THRESHOLD:
                continue
            if p["model"] == "mismatch" and raw < MM_MIN_CONFIDENCE:
                continue
            q = dict(p)
            q["predicted_prob"] = round(cal, 4)
            gated.append(q)
        print(f"\n  Eval predictions passing live gates: {len(gated)}")

        # De-duplicate: one prediction per (fixture, market)
        deduped = deduplicate_predictions(gated)
        print(f"  After dedup: {len(deduped)} unique (fixture, market) predictions")
        print(f"  Unique fixtures with predictions: "
              f"{len(set(p['fixture_id'] for p in deduped))}")

        print("\nComputing calibration (eval folds only)...")
        report = compute_calibration(deduped)

        # Same picks scored on RAW probabilities, to show the maps' effect
        raw_scored = [dict(p, predicted_prob=p["predicted_prob_raw"])
                      for p in gated]
        raw_deduped = deduplicate_predictions(raw_scored)
        rdf = pd.DataFrame(raw_deduped)
        if not rdf.empty:
            rdf["actual_int"] = rdf["actual"].astype(int)
            report["eval_raw_summary"] = {
                "n": len(rdf),
                "brier_score": round(float(((rdf["predicted_prob"] -
                                             rdf["actual_int"]) ** 2).mean()), 4),
                "avg_predicted": round(float(rdf["predicted_prob"].mean()), 4),
                "overall_hit_rate": round(float(rdf["actual_int"].mean()), 4),
                "overconfidence": round(float(rdf["predicted_prob"].mean() -
                                              rdf["actual_int"].mean()), 4),
            }

        # ── Shipped maps: method validated above, refit on ALL folds so the
        # live maps use every raw prediction, including the most recent ──
        shipped_models = {}
        for model in ("bayesian", "poisson", "mismatch"):
            mp = [p for p in all_predictions if p["model"] == model]
            shipped_models[model] = fit_pava_map(
                [p["predicted_prob_raw"] for p in mp],
                [float(p["actual"]) for p in mp])
        shipped_maps = {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "method": "walk-forward equal-count bins + PAVA, refit on all folds",
            "months": months,
            "fold_days": fold_days,
            "n_folds": n_folds,
            "validation": "fit on folds 1-%d, evaluated on folds %d-%d" % (
                n_fit, n_fit + 1, n_folds),
            "models": shipped_models,
        }

        eval_fixtures = all_test_fixtures[all_test_fixtures["fold"] > n_fit]
        report["raw_prediction_count"] = len(all_predictions)
        report["base_rates"] = compute_base_rates(eval_fixtures)
        report["predictions"] = deduped
        report["method"] = "walk-forward (calibration fit/eval fold split)"
        report["fold_days"] = fold_days
        report["n_folds"] = n_folds
        report["n_fit_folds"] = n_fit
        report["fit_maps"] = fit_maps
        report["shipped_maps"] = shipped_maps

        return report

    finally:
        conn.close()


def print_report(report: dict) -> None:
    if "error" in report:
        print(f"\nError: {report['error']}")
        return

    print(f"\n{'=' * 70}")
    print(f"  WALK-FORWARD CALIBRATION REPORT")
    print(f"  Method: {report.get('method', 'unknown')} | "
          f"{report.get('n_folds', '?')} folds of "
          f"{report.get('fold_days', '?')} days | "
          f"maps fit on folds 1-{report.get('n_fit_folds', '?')}")
    print(f"  Metrics below are EVAL FOLDS ONLY (never seen by the maps)")
    print(f"{'=' * 70}")
    print(f"  De-duplicated predictions: {report['n']}")
    print(f"  Unique fixtures:           {report.get('unique_fixtures', '?')}")
    print(f"  Raw predictions (all folds, pre-gate): "
          f"{report.get('raw_prediction_count', '?')}")
    print(f"  Brier score:       {report['brier_score']:.4f}  "
          f"(lower = better, 0 = perfect)")
    print(f"  Avg predicted:     {report['avg_predicted']:.1%}")
    print(f"  Actual hit rate:   {report['overall_hit_rate']:.1%}")
    oc = report['overconfidence']
    label = ("OVERCONFIDENT" if oc > 0.02 else
             "UNDERCONFIDENT" if oc < -0.02 else "WELL CALIBRATED")
    print(f"  Overconfidence:    {oc:+.1%}  ({label})")

    raw_sum = report.get("eval_raw_summary")
    if raw_sum:
        print(f"\n  Same picks, RAW (uncalibrated) probabilities:")
        print(f"    brier={raw_sum['brier_score']:.4f}  "
              f"predicted={raw_sum['avg_predicted']:.1%}  "
              f"actual={raw_sum['overall_hit_rate']:.1%}  "
              f"gap={raw_sum['overconfidence']:+.1%}")

    br = report.get("base_rates", {})
    if br:
        print(f"\n{'─' * 70}")
        print(f"  BASE RATES (what you'd get picking blind)")
        print(f"{'─' * 70}")
        print(f"  Home team scores ≥1:  {br.get('home_scores', 0):.1%}")
        print(f"  Away team scores ≥1:  {br.get('away_scores', 0):.1%}")
        print(f"  Match Over 1.5:       {br.get('over_1.5', 0):.1%}")
        print(f"  Match Over 2.5:       {br.get('over_2.5', 0):.1%}")
        print(f"  Match Under 3.5:      {br.get('under_3.5', 0):.1%}")

    print(f"\n{'─' * 70}")
    print(f"  MODEL PARTICIPATION (which models contributed)")
    print(f"{'─' * 70}")
    for model, stats in report.get("per_model", {}).items():
        print(f"  {model:<12s} in {stats['n']:>6d} predictions  "
              f"brier={stats['brier']:.4f}  "
              f"pred={stats['avg_predicted']:.1%}  "
              f"actual={stats['actual_hit_rate']:.1%}  "
              f"gap={stats['overconfidence']:+.1%}")

    print(f"\n{'─' * 70}")
    print(f"  CONSENSUS TIERS")
    print(f"{'─' * 70}")
    for tier, stats in report.get("consensus", {}).items():
        print(f"  {tier:<6s} N={stats['n']:>5d}  "
              f"predicted={stats['avg_predicted']:.1%}  "
              f"actual={stats['actual_hit_rate']:.1%}  "
              f"brier={stats['brier']:.4f}")

    print(f"\n{'─' * 70}")
    print(f"  PER-MARKET BREAKDOWN")
    print(f"{'─' * 70}")
    print(f"  {'Market':<14s} {'N':>6s} {'Predicted':>10s} "
          f"{'Actual':>8s} {'Gap':>8s} {'vs Base':>10s}")
    base = report.get("base_rates", {})
    base_map = {
        "over_match": base.get("over_1.5", 0),
        "under": base.get("under_3.5", 0),
        "team_scores": (base.get("home_scores", 0) + base.get("away_scores", 0)) / 2,
    }
    for mt, stats in report.get("per_market", {}).items():
        gap = stats['avg_predicted'] - stats['actual_hit_rate']
        base_rate = base_map.get(mt, 0)
        lift = stats['actual_hit_rate'] - base_rate if base_rate else 0
        print(f"  {mt:<14s} {stats['n']:>6d} {stats['avg_predicted']:>9.1%} "
              f"{stats['actual_hit_rate']:>7.1%} {gap:>+7.1%}"
              f"  {f'lift:{lift:+.1%}' if base_rate else ''}")

    print(f"\n{'─' * 70}")
    print(f"  CALIBRATION CURVE (predicted vs actual)")
    print(f"{'─' * 70}")
    print(f"  {'Bin':>10s} {'N':>6s} {'Predicted':>10s} "
          f"{'Actual':>8s} {'Gap':>8s} {'Visual'}")
    for b in report.get("calibration_bins", []):
        bar_len = int(b["actual_rate"] * 40)
        bar = "█" * bar_len + "░" * (40 - bar_len)
        print(f"  {b['bin_lo']:.0%}-{b['bin_hi']:.0%}  {b['n']:>5d} "
              f"{b['predicted_avg']:>9.1%} {b['actual_rate']:>7.1%} "
              f"{b['gap']:>+7.1%}  {bar}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Walk-forward calibration backtest")
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--fold-days", type=int, default=14)
    parser.add_argument("--cal-frac", type=float, default=CAL_FRAC_DEFAULT,
                        help="Fraction of folds used to fit calibration maps")
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--no-save-maps", action="store_true",
                        help="Skip writing calibration_maps.json for live use")
    args = parser.parse_args()

    report = run_walk_forward(months=args.months, fold_days=args.fold_days,
                              cal_frac=args.cal_frac)
    print_report(report)

    out_path = Path(args.output) if args.output else (
        Path(__file__).parent / "calibration_report.json")
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport saved to {out_path}")

    if not args.no_save_maps and report.get("shipped_maps"):
        with open(MAPS_PATH, "w") as f:
            json.dump(report["shipped_maps"], f, indent=2)
        print(f"Live calibration maps saved to {MAPS_PATH}")
