"""
Model calibration backtest — IN-SAMPLE ONLY.

WARNING: this script trains every model on the SAME fixtures it then
evaluates (no date cutoffs anywhere): Bayesian beliefs include the
outcome of the fixture being predicted, Poisson trains on the test set,
and mismatch hit rates include test fixtures. Numbers here are an upper
bound / smoke test, NOT evidence of skill.

Use engine.eval.calibration_wf (walk-forward, leak-free) for anything
that informs calibration maps, thresholds, or real betting decisions.

Usage:
    python -m engine.eval.calibration
    python -m engine.eval.calibration --months 3 --output my_report.json
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
    load_event_history,
)
from engine.eval.predict_upcoming import (
    CONFIDENCE_CAP,
    LEAGUE_PENALTIES,
    _calibrate_prob,
)
from etl.src.logger import get_logger

logger = get_logger(__name__)


def _get_conn():
    return psycopg2.connect(
        dbname="ballistics",
        user="ballistics_user",
        password="ronaldo",
        host="localhost",
        port=5432,
    )


def _actual_outcome(row: pd.Series, market: str) -> bool | None:
    """Given a completed fixture row and a market string, return True/False."""
    hg = row.get("goals_home_ft")
    ag = row.get("goals_away_ft")
    if hg is None or ag is None:
        return None
    hg, ag = int(hg), int(ag)
    total = hg + ag

    if "Match Over" in market:
        line = float(market.split()[-1])
        return total > line
    if "Match Under" in market:
        line = float(market.split()[-1])
        return total < line
    if "Over 0.5" in market:
        team_name = market.replace(" Over 0.5", "").strip()
        if team_name == row.get("home_team_name", ""):
            return hg >= 1
        if team_name == row.get("away_team_name", ""):
            return ag >= 1
        return None
    if "Over 1.5" in market and "Match" not in market:
        team_name = market.replace(" Over 1.5", "").strip()
        if team_name == row.get("home_team_name", ""):
            return hg >= 2
        if team_name == row.get("away_team_name", ""):
            return ag >= 2
        return None
    if "Win" in market:
        team_name = market.replace(" Win", "").strip()
        if team_name == row.get("home_team_name", ""):
            return hg > ag
        if team_name == row.get("away_team_name", ""):
            return ag > hg
        return None
    if "BTTS" in market:
        return hg >= 1 and ag >= 1

    return None


def load_completed_fixtures(conn, months: int = 3) -> pd.DataFrame:
    cutoff = datetime.now(timezone.utc) - timedelta(days=months * 30)
    df = pd.read_sql("""
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
    return df


def _build_bayesian_predictions(conn, fixtures: pd.DataFrame) -> list[dict]:
    """Run Bayesian model on completed fixtures as if they were upcoming."""
    results = []
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

    all_rf_ids = set(fixtures["home_team_id"].tolist() + fixtures["away_team_id"].tolist())
    all_fh_ids = {id_map.get(rf, rf) for rf in all_rf_ids}

    league_map_df = pd.read_sql("""
        SELECT DISTINCT r.api_league_id, f.league_name AS league
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head f ON f.fixture_id = r.fixture_id AND f.is_home
    """, conn)
    lmap = dict(zip(league_map_df["api_league_id"], league_map_df["league"]))

    for ctx in ("home", "away"):
        event_df = load_event_history(conn, context=ctx)
        event_df = event_df[
            event_df["team_id"].isin(all_fh_ids)
            & event_df["event_name"].isin(BETTING_EVENTS)
        ]
        base_rates = compute_league_base_rates(conn, context=ctx)
        beliefs = compute_beliefs(event_df, base_rates)

        belief_by_team = defaultdict(list)
        for b in beliefs:
            b.context = ctx
            belief_by_team[b.team_id].append(b)

        for _, fix in fixtures.iterrows():
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
                if b.posterior < MIN_POSTERIOR_INTERESTING:
                    continue

                event = b.event_name
                market = None
                if event == "Over_1_5":
                    market = "Match Over 1.5"
                elif event == "Over_2_5":
                    market = "Match Over 2.5"
                elif event == "Under_2_5":
                    market = "Match Under 2.5"
                elif event == "Under_3_5":
                    market = "Match Under 3.5"
                elif event == "score_1goal":
                    market = f"{team_name} Over 0.5"
                elif event == "score_2goals":
                    market = f"{team_name} Over 1.5"
                elif event == "concede_1":
                    market = f"{opp_name} Over 0.5"
                elif event == "BTTS":
                    market = "BTTS Yes"

                if market:
                    outcome = _actual_outcome(fix, market)
                    if outcome is not None:
                        results.append({
                            "fixture_id": fid,
                            "model": "bayesian",
                            "market": market,
                            "predicted_prob": round(b.posterior, 4),
                            "actual": outcome,
                        })

    return results


def _build_poisson_predictions(conn, fixtures: pd.DataFrame) -> list[dict]:
    """Run Poisson model on completed fixtures."""
    from engine.eval.predict_upcoming import (
        OVER_THRESHOLD,
        load_historical,
        prepare_features,
        compute_form_features,
        compute_strength_features,
        compute_rest_days,
        assign_clusters_seasonal,
        _fit_poisson_lambda,
        _predict_poisson_over,
        OVER_COLS,
        UNDER_COLS,
        MIN_TRAIN_PER_CLUSTER,
        _load_league_map,
        _build_team_id_map,
        _get_latest_elo,
    )

    results = []
    hist_raw = load_historical(conn)
    hist = compute_form_features(prepare_features(hist_raw))

    league_map = _load_league_map(conn)
    team_id_map = _build_team_id_map(conn)

    test_df = fixtures.copy()
    test_df["league"] = test_df["api_league_id"].map(league_map)
    test_df = test_df[test_df["league"].notna()].copy()

    if test_df.empty:
        return results

    all_teams = set(test_df["home_team_id"].tolist() + test_df["away_team_id"].tolist())
    elo_map = _get_latest_elo(conn, list(all_teams), id_map=team_id_map)
    default_elo = 1500.0
    test_df["home_elo"] = test_df["home_team_id"].map(elo_map).fillna(default_elo)
    test_df["away_elo"] = test_df["away_team_id"].map(elo_map).fillna(default_elo)
    test_df["abs_gap"] = (test_df["home_elo"] - test_df["away_elo"]).abs()
    test_df["max_elo"] = test_df[["home_elo", "away_elo"]].max(axis=1)
    test_df["min_elo"] = test_df[["home_elo", "away_elo"]].min(axis=1)
    test_df["elo_sum"] = test_df["home_elo"] + test_df["away_elo"]
    test_df["home_elo_gap"] = test_df["home_elo"] - test_df["away_elo"]
    test_df["kickoff_utc"] = pd.to_datetime(test_df["kickoff_utc"], utc=True)

    from engine.eval.cluster_backtest import ELO_GAP_SENSITIVITY, _MEDIAN_SENSITIVITY
    sensitivity = test_df["league"].map(ELO_GAP_SENSITIVITY).fillna(_MEDIAN_SENSITIVITY)
    test_df["elo_gap_scaled"] = test_df["abs_gap"] * sensitivity

    test_s = compute_strength_features(hist, test_df)
    test_s = compute_rest_days(test_s)

    train_s = compute_strength_features(hist, hist)
    train_s = compute_rest_days(train_s)
    train_s["cluster"] = assign_clusters_seasonal(train_s, train_s)
    test_s["cluster"] = assign_clusters_seasonal(train_s, test_s)

    for line in [1.5, 2.5, 3.5]:
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

        under_probs = 1.0 - over_probs

        for i, (_, row) in enumerate(test_s.iterrows()):
            fid = int(row["fixture_id"])
            fix_row = fixtures[fixtures["fixture_id"] == fid]
            if fix_row.empty:
                continue
            fix_row = fix_row.iloc[0]
            league = row.get("league", "")

            over_raw = float(over_probs[i])
            under_raw = float(under_probs[i])
            over_cal = _calibrate_prob(over_raw, league)
            under_cal = _calibrate_prob(under_raw, league)

            over_market = f"Match Over {line}"
            under_market = f"Match Under {line}"

            over_outcome = _actual_outcome(fix_row, over_market)
            under_outcome = _actual_outcome(fix_row, under_market)

            if over_outcome is not None and over_cal >= 0.65:
                results.append({
                    "fixture_id": fid,
                    "model": "poisson",
                    "market": over_market,
                    "predicted_prob": round(over_cal, 4),
                    "actual": over_outcome,
                })
            if under_outcome is not None and under_cal >= 0.65:
                results.append({
                    "fixture_id": fid,
                    "model": "poisson",
                    "market": under_market,
                    "predicted_prob": round(under_cal, 4),
                    "actual": under_outcome,
                })

    return results


def _build_mismatch_predictions(conn, fixtures: pd.DataFrame) -> list[dict]:
    """Run Mismatch Elo model on completed fixtures."""
    from engine.eval.predict_mismatch import (
        build_hit_rates,
        _load_league_map as mm_load_league_map,
        _build_team_id_map as mm_build_id_map,
        _get_latest_elo as mm_get_elo,
        _get_gap_bin,
        _load_h2h_records,
        _load_team_strength,
        _h2h_adjustment,
        _lookup_hit_rate,
        _strength_adjustment,
        MIN_CONFIDENCE,
        MIN_SAMPLE,
    )

    results = []
    hit_rates = build_hit_rates(conn)
    if hit_rates.empty:
        return results

    league_map = mm_load_league_map(conn)
    test_df = fixtures.copy()
    test_df["league"] = test_df["api_league_id"].map(league_map)
    test_df = test_df[test_df["league"].notna()].copy()
    if test_df.empty:
        return results

    id_map = mm_build_id_map(conn)
    all_teams = set(test_df["home_team_id"].tolist() + test_df["away_team_id"].tolist())
    elo_map = mm_get_elo(conn, list(all_teams), id_map)

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
        fixture_info.append((row, h_elo, a_elo, gap, bin_info, fav_is_home, fav_rf, dog_rf))

    if not fixture_info:
        return results

    matchups = [(f[6], f[7]) for f in fixture_info]
    h2h_map = _load_h2h_records(conn, id_map, matchups)
    strength_map = _load_team_strength(conn, id_map)

    for row, h_elo, a_elo, gap, bin_info, fav_is_home, fav_rf, dog_rf in fixture_info:
        gap_bin, _, _ = bin_info
        fav_name = row["home_team_name"] if fav_is_home else row["away_team_name"]
        dog_name = row["away_team_name"] if fav_is_home else row["home_team_name"]

        h2h_w, h2h_d, h2h_l = h2h_map.get((fav_rf, dog_rf), (0, 0, 0))
        h2h_boost, _ = _h2h_adjustment(h2h_w, h2h_d, h2h_l)
        fav_strength = strength_map.get(fav_rf)
        dog_strength = strength_map.get(dog_rf)

        fix_row = fixtures[fixtures["fixture_id"] == row["fixture_id"]]
        if fix_row.empty:
            continue
        fix_row = fix_row.iloc[0]

        for mkt in ["fav_to_score", "fav_over_1.5", "over_1.5", "over_2.5"]:
            conf, n = _lookup_hit_rate(hit_rates, row.get("league", ""), gap_bin, mkt)
            str_adj = _strength_adjustment(mkt, fav_strength, dog_strength)
            adjusted = max(min(conf + str_adj, 0.99), 0.0)
            if adjusted < MIN_CONFIDENCE or n < MIN_SAMPLE:
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
                    "model": "mismatch",
                    "market": market,
                    "predicted_prob": round(adjusted, 4),
                    "actual": outcome,
                })

        win_conf, win_n = _lookup_hit_rate(hit_rates, row.get("league", ""), gap_bin, "fav_win")
        adjusted_win = min(win_conf + h2h_boost, 0.95)
        if adjusted_win >= MIN_CONFIDENCE and win_n >= MIN_SAMPLE:
            market = f"{fav_name} Win"
            outcome = _actual_outcome(fix_row, market)
            if outcome is not None:
                results.append({
                    "fixture_id": int(row["fixture_id"]),
                    "model": "mismatch",
                    "market": market,
                    "predicted_prob": round(adjusted_win, 4),
                    "actual": outcome,
                })

    return results


def compute_calibration(predictions: list[dict], n_bins: int = 10) -> dict:
    """Compute calibration metrics from prediction list."""
    df = pd.DataFrame(predictions)
    if df.empty:
        return {"n": 0}

    df["actual_int"] = df["actual"].astype(int)

    # Overall Brier score
    brier = float(((df["predicted_prob"] - df["actual_int"]) ** 2).mean())

    # Calibration bins
    bins = np.linspace(0.5, 1.0, n_bins + 1)
    bin_data = []
    for i in range(len(bins) - 1):
        lo, hi = bins[i], bins[i + 1]
        mask = (df["predicted_prob"] >= lo) & (df["predicted_prob"] < hi)
        subset = df[mask]
        if len(subset) == 0:
            continue
        bin_data.append({
            "bin_lo": round(lo, 3),
            "bin_hi": round(hi, 3),
            "bin_mid": round((lo + hi) / 2, 3),
            "n": len(subset),
            "predicted_avg": round(float(subset["predicted_prob"].mean()), 4),
            "actual_rate": round(float(subset["actual_int"].mean()), 4),
            "gap": round(float(subset["predicted_prob"].mean() - subset["actual_int"].mean()), 4),
        })

    # Per-model breakdown
    per_model = {}
    for model, grp in df.groupby("model"):
        model_brier = float(((grp["predicted_prob"] - grp["actual_int"]) ** 2).mean())
        hit_rate = float(grp["actual_int"].mean())
        avg_conf = float(grp["predicted_prob"].mean())
        per_model[model] = {
            "n": len(grp),
            "brier": round(model_brier, 4),
            "avg_predicted": round(avg_conf, 4),
            "actual_hit_rate": round(hit_rate, 4),
            "overconfidence": round(avg_conf - hit_rate, 4),
        }

    # Consensus breakdown (picks where 2+ models agree)
    consensus_keys = df.groupby(["fixture_id", "market"]).agg(
        n_models=("model", "nunique"),
        avg_conf=("predicted_prob", "mean"),
        actual=("actual_int", "first"),
    ).reset_index()

    duo = consensus_keys[consensus_keys["n_models"] >= 2]
    solo = consensus_keys[consensus_keys["n_models"] == 1]

    consensus_stats = {}
    if len(duo) > 0:
        consensus_stats["duo_plus"] = {
            "n": len(duo),
            "avg_predicted": round(float(duo["avg_conf"].mean()), 4),
            "actual_hit_rate": round(float(duo["actual"].mean()), 4),
            "brier": round(float(((duo["avg_conf"] - duo["actual"]) ** 2).mean()), 4),
        }
    if len(solo) > 0:
        consensus_stats["solo"] = {
            "n": len(solo),
            "avg_predicted": round(float(solo["avg_conf"].mean()), 4),
            "actual_hit_rate": round(float(solo["actual"].mean()), 4),
            "brier": round(float(((solo["avg_conf"] - solo["actual"]) ** 2).mean()), 4),
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
            "brier": round(float(((grp["predicted_prob"] - grp["actual_int"]) ** 2).mean()), 4),
        }

    return {
        "n": len(df),
        "brier_score": round(brier, 4),
        "avg_predicted": round(float(df["predicted_prob"].mean()), 4),
        "overall_hit_rate": round(float(df["actual_int"].mean()), 4),
        "overconfidence": round(float(df["predicted_prob"].mean() - df["actual_int"].mean()), 4),
        "calibration_bins": bin_data,
        "per_model": per_model,
        "consensus": consensus_stats,
        "per_market": per_market,
    }


def run_calibration(months: int = 3) -> dict:
    conn = _get_conn()
    try:
        print("=" * 60)
        print("  CALIBRATION BACKTEST — IN-SAMPLE (models see test outcomes)")
        print("  For honest numbers use: python -m engine.eval.calibration_wf")
        print(f"  Period: last {months} months of completed fixtures")
        print("=" * 60)

        print("\nLoading completed fixtures...")
        fixtures = load_completed_fixtures(conn, months=months)
        print(f"  {len(fixtures)} completed fixtures loaded")
        if fixtures.empty:
            return {"error": "No completed fixtures found"}

        all_predictions = []

        print("\n[1/3] Bayesian Streaks...")
        try:
            bayesian_preds = _build_bayesian_predictions(conn, fixtures)
            all_predictions.extend(bayesian_preds)
            print(f"  {len(bayesian_preds)} predictions generated")
        except Exception as e:
            print(f"  Failed: {e}")

        print("\n[2/3] Poisson GLM...")
        try:
            poisson_preds = _build_poisson_predictions(conn, fixtures)
            all_predictions.extend(poisson_preds)
            print(f"  {len(poisson_preds)} predictions generated")
        except Exception as e:
            print(f"  Failed: {e}")

        print("\n[3/3] Mismatch Elo...")
        try:
            mismatch_preds = _build_mismatch_predictions(conn, fixtures)
            all_predictions.extend(mismatch_preds)
            print(f"  {len(mismatch_preds)} predictions generated")
        except Exception as e:
            print(f"  Failed: {e}")

        print(f"\nTotal predictions: {len(all_predictions)}")

        fix_lookup = fixtures.set_index("fixture_id").to_dict("index")
        for pred in all_predictions:
            finfo = fix_lookup.get(pred["fixture_id"], {})
            pred["home"] = finfo.get("home_team_name", "")
            pred["away"] = finfo.get("away_team_name", "")
            pred["score"] = f"{int(finfo.get('goals_home_ft', 0))}-{int(finfo.get('goals_away_ft', 0))}"
            pred["kickoff"] = str(finfo.get("kickoff_utc", ""))[:16]
            pred["league_id"] = int(finfo.get("api_league_id", 0))

        print("\nComputing calibration...")
        report = compute_calibration(all_predictions)
        report["predictions"] = all_predictions

        return report

    finally:
        conn.close()


def print_report(report: dict) -> None:
    if "error" in report:
        print(f"\nError: {report['error']}")
        return

    print(f"\n{'=' * 70}")
    print(f"  CALIBRATION REPORT")
    print(f"{'=' * 70}")
    print(f"  Total predictions: {report['n']}")
    print(f"  Brier score:       {report['brier_score']:.4f}  (lower = better, 0 = perfect)")
    print(f"  Avg predicted:     {report['avg_predicted']:.1%}")
    print(f"  Actual hit rate:   {report['overall_hit_rate']:.1%}")
    oc = report['overconfidence']
    label = "OVERCONFIDENT" if oc > 0.02 else "UNDERCONFIDENT" if oc < -0.02 else "WELL CALIBRATED"
    print(f"  Overconfidence:    {oc:+.1%}  ({label})")

    print(f"\n{'─' * 70}")
    print(f"  PER-MODEL BREAKDOWN")
    print(f"{'─' * 70}")
    print(f"  {'Model':<12s} {'N':>6s} {'Brier':>7s} {'Predicted':>10s} {'Actual':>8s} {'Gap':>8s}")
    for model, stats in report.get("per_model", {}).items():
        print(f"  {model:<12s} {stats['n']:>6d} {stats['brier']:>7.4f} "
              f"{stats['avg_predicted']:>9.1%} {stats['actual_hit_rate']:>7.1%} "
              f"{stats['overconfidence']:>+7.1%}")

    print(f"\n{'─' * 70}")
    print(f"  CONSENSUS vs SOLO")
    print(f"{'─' * 70}")
    for tier, stats in report.get("consensus", {}).items():
        print(f"  {tier:<12s} N={stats['n']:>5d}  predicted={stats['avg_predicted']:.1%}  "
              f"actual={stats['actual_hit_rate']:.1%}  brier={stats['brier']:.4f}")

    print(f"\n{'─' * 70}")
    print(f"  PER-MARKET BREAKDOWN")
    print(f"{'─' * 70}")
    print(f"  {'Market':<14s} {'N':>6s} {'Predicted':>10s} {'Actual':>8s} {'Gap':>8s}")
    for mt, stats in report.get("per_market", {}).items():
        gap = stats['avg_predicted'] - stats['actual_hit_rate']
        print(f"  {mt:<14s} {stats['n']:>6d} {stats['avg_predicted']:>9.1%} "
              f"{stats['actual_hit_rate']:>7.1%} {gap:>+7.1%}")

    print(f"\n{'─' * 70}")
    print(f"  CALIBRATION CURVE (predicted vs actual)")
    print(f"{'─' * 70}")
    print(f"  {'Bin':>10s} {'N':>6s} {'Predicted':>10s} {'Actual':>8s} {'Gap':>8s} {'Visual'}")
    for b in report.get("calibration_bins", []):
        bar_len = int(b["actual_rate"] * 40)
        bar = "█" * bar_len + "░" * (40 - bar_len)
        print(f"  {b['bin_lo']:.0%}-{b['bin_hi']:.0%}  {b['n']:>5d} "
              f"{b['predicted_avg']:>9.1%} {b['actual_rate']:>7.1%} "
              f"{b['gap']:>+7.1%}  {bar}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Model calibration backtest")
    parser.add_argument("--months", type=int, default=3, help="Months of history to backtest")
    parser.add_argument("--output", type=str, default=None, help="Output JSON path")
    args = parser.parse_args()

    report = run_calibration(months=args.months)
    print_report(report)

    if args.output:
        out_path = Path(args.output)
    else:
        # Never write to calibration_report.json — that file belongs to the
        # walk-forward backtest and must not be overwritten by in-sample runs.
        out_path = Path(__file__).parent / "calibration_report_insample.json"

    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport saved to {out_path}")
