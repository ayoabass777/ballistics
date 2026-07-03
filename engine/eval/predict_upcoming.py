"""
Predict over/under probabilities for upcoming (unplayed) fixtures.

Trains the over/under Poisson models on all historical data, then scores
upcoming NS fixtures. Outputs picks filtered by confidence threshold.

Usage:
    DB_HOST=localhost python -m engine.eval.predict_upcoming
    DB_HOST=localhost python -m engine.eval.predict_upcoming --days 7 --threshold 0.60
"""

from __future__ import annotations

import argparse
import warnings
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import psycopg2
import statsmodels.api as sm
from scipy.stats import poisson as poisson_dist

CALIBRATED_THRESHOLD = 0.65

from engine.eval.cluster_backtest import (
    OVER_COLS,
    UNDER_COLS_TUNED as UNDER_COLS,
    OVER_THRESHOLD,
    UNDER_THRESHOLD,
    ELO_GAP_SENSITIVITY,
    _MEDIAN_SENSITIVITY,
    assign_clusters_seasonal,
    compute_form_features,
    compute_rest_days,
    compute_strength_features,
    prepare_features,
    _fit_poisson_lambda,
    MIN_TRAIN_PER_CLUSTER,
)
from engine.eval.league_clustering import build_league_profiles
from etl.src.logger import get_logger


def _predict_poisson_over(params: np.ndarray, X: np.ndarray,
                          line: float) -> np.ndarray:
    X_c = sm.add_constant(X.astype(float), has_constant="add")
    lam = np.exp(X_c @ params)
    k = int(line)
    return 1.0 - poisson_dist.cdf(k, lam)

logger = get_logger(__name__)

CONFIDENCE_CAP = 0.85

LEAGUE_PENALTIES = {
    "2. Bundesliga": 0.136,
    "1. Division": 0.123,
    "Meistriliiga": 0.079,
    "Superliga": 0.071,
    "Veikkausliiga": 0.072,
    "First Division": 0.064,
    "USL League One": 0.091,
    "Premiership": 0.062,
    "Eliteserien": 0.015,
    "Jupiler Pro League": 0.018,
    "Eredivisie": 0.022,
    "Ligue 1": 0.018,
    "USL Championship": 0.026,
    "Primeira Liga": 0.036,
    "Süper Lig": 0.023,
    "Virsliga": 0.026,
    "A Lyga": 0.031,
    "Primera División": 0.040,
    "Erovnuli Liga": 0.033,
    "Allsvenskan": 0.013,
    "Premier League": 0.020,
    "Premier Division": 0.037,
    "Championship": 0.037,
    "Segunda Division": 0.063,
    "Serie B": 0.031,
    "1. Deild": 0.256,
}

from engine.eval.calibration_maps import apply_points, load_points

_POISSON_POINTS = load_points("poisson")


def _calibrate_prob(raw_prob: float, league: str) -> float:
    """Calibrate a raw Poisson probability.

    Prefers the monotonic map fitted by calibration_wf on held-out folds
    (calibration_maps.json); falls back to the legacy cap + per-league
    penalty when the file is absent.
    """
    if _POISSON_POINTS:
        return apply_points(_POISSON_POINTS, raw_prob)
    capped = min(raw_prob, CONFIDENCE_CAP)
    penalty = LEAGUE_PENALTIES.get(league, 0.0)
    return max(capped - penalty, 0.0)


SOURCE_TABLE = "raw.fact_head_to_head"
ELO_TABLE = "elo.team_elo_history"


def _get_conn():
    from engine.eval.db import get_conn
    return get_conn()


def load_historical(conn) -> pd.DataFrame:
    sql = f"""
        SELECT
            f.fixture_id, f.kickoff_utc, f.season,
            f.league_name AS league,
            f.team_id AS home_team_id, f.opponent_id AS away_team_id,
            f.team_name AS home_team, f.opponent_name AS away_team,
            f.goals_for AS home_goals, f.goals_against AS away_goals,
            COALESCE(eh.rating_before_flat, eh.rating_before) AS home_elo,
            COALESCE(ea.rating_before_flat, ea.rating_before) AS away_elo
        FROM {SOURCE_TABLE} f
        JOIN {ELO_TABLE} eh ON eh.team_id = f.team_id AND eh.fixture_id = f.fixture_id
        JOIN {ELO_TABLE} ea ON ea.team_id = f.opponent_id AND ea.fixture_id = f.fixture_id
        WHERE f.is_played AND f.is_home
        ORDER BY f.kickoff_utc
    """
    return pd.read_sql(sql, conn)


def load_upcoming(conn, days: int = 7) -> pd.DataFrame:
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end = today + timedelta(days=days)
    sql = f"""
        SELECT
            r.fixture_id, r.kickoff_utc, r.season,
            r.home_team_id, r.home_team_name AS home_team,
            r.away_team_id, r.away_team_name AS away_team,
            r.api_league_id
        FROM raw.raw_fixtures r
        WHERE r.fixture_status = 'NS'
          AND r.kickoff_utc BETWEEN %s AND %s
        ORDER BY r.kickoff_utc
    """
    return pd.read_sql(sql, conn, params=[today, end])


def _load_league_map(conn) -> dict:
    """Build api_league_id -> league_name from historical fixture join."""
    df = pd.read_sql("""
        SELECT DISTINCT r.api_league_id, f.league_name
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head f ON f.fixture_id = r.fixture_id AND f.is_home
    """, conn)
    return dict(zip(df["api_league_id"], df["league_name"]))


def _map_league_names(upcoming: pd.DataFrame, league_map: dict) -> pd.DataFrame:
    out = upcoming.copy()
    out["league"] = out["api_league_id"].map(league_map)
    return out


def _build_team_id_map(conn) -> dict:
    """Map raw_fixtures team IDs to fact_head_to_head team IDs (used by Elo)."""
    df = pd.read_sql(f"""
        SELECT DISTINCT r.home_team_id AS rf_id, f.team_id AS fh_id
        FROM raw.raw_fixtures r
        JOIN {SOURCE_TABLE} f ON f.fixture_id = r.fixture_id AND f.is_home
        UNION
        SELECT DISTINCT r.away_team_id, f.opponent_id
        FROM raw.raw_fixtures r
        JOIN {SOURCE_TABLE} f ON f.fixture_id = r.fixture_id AND f.is_home
    """, conn)
    return dict(zip(df["rf_id"], df["fh_id"]))


def _get_latest_elo(conn, team_ids: list, id_map: dict = None) -> dict:
    """Get latest Elo rating for each team, translating IDs if needed."""
    if not team_ids:
        return {}
    sql = f"""
        SELECT DISTINCT ON (team_id)
            team_id, COALESCE(rating_after_flat, rating_after) AS rating
        FROM {ELO_TABLE}
        ORDER BY team_id, kickoff_utc DESC
    """
    df = pd.read_sql(sql, conn)
    elo_by_fh_id = dict(zip(df["team_id"], df["rating"]))
    if id_map is None:
        return elo_by_fh_id
    result = {}
    for rf_id in team_ids:
        fh_id = id_map.get(rf_id, rf_id)
        if fh_id in elo_by_fh_id:
            result[rf_id] = elo_by_fh_id[fh_id]
    return result


def predict_upcoming(days: int = 7, threshold: float = None) -> pd.DataFrame:
    if threshold is None:
        threshold = OVER_THRESHOLD
    conn = _get_conn()
    try:
        print("Loading historical data...")
        hist_raw = load_historical(conn)
        hist = compute_form_features(prepare_features(hist_raw))
        print(f"  {len(hist)} historical matches loaded")

        print("Loading upcoming fixtures...")
        upcoming_raw = load_upcoming(conn, days=days)
        if upcoming_raw.empty:
            print("  No upcoming fixtures found.")
            return pd.DataFrame()
        print(f"  {len(upcoming_raw)} upcoming fixtures in next {days} days")

        league_map = _load_league_map(conn)
        upcoming = _map_league_names(upcoming_raw, league_map)
        unknown = upcoming[upcoming["league"].isna()]
        if not unknown.empty:
            print(f"  Dropping {len(unknown)} fixtures with unknown leagues")
            upcoming = upcoming[upcoming["league"].notna()].copy()

        if upcoming.empty:
            print("  No fixtures with known leagues.")
            return pd.DataFrame()

        # Get latest Elo for each team (translate raw_fixtures IDs to Elo IDs)
        team_id_map = _build_team_id_map(conn)
        all_teams = set(upcoming["home_team_id"].tolist() + upcoming["away_team_id"].tolist())
        elo_map = _get_latest_elo(conn, list(all_teams), id_map=team_id_map)
        default_elo = 1500.0
        upcoming["home_elo"] = upcoming["home_team_id"].map(elo_map).fillna(default_elo)
        upcoming["away_elo"] = upcoming["away_team_id"].map(elo_map).fillna(default_elo)

        # Prepare features (same as prepare_features but without goals)
        upcoming["abs_gap"] = (upcoming["home_elo"] - upcoming["away_elo"]).abs()
        upcoming["max_elo"] = upcoming[["home_elo", "away_elo"]].max(axis=1)
        upcoming["min_elo"] = upcoming[["home_elo", "away_elo"]].min(axis=1)
        upcoming["elo_sum"] = upcoming["home_elo"] + upcoming["away_elo"]
        upcoming["home_elo_gap"] = upcoming["home_elo"] - upcoming["away_elo"]
        upcoming["kickoff_utc"] = pd.to_datetime(upcoming["kickoff_utc"], utc=True)
        sensitivity = upcoming["league"].map(ELO_GAP_SENSITIVITY).fillna(_MEDIAN_SENSITIVITY)
        upcoming["elo_gap_scaled"] = upcoming["abs_gap"] * sensitivity

        league_goal_avg = hist.groupby("league")["total_goals"].mean().to_dict()

        print("Computing features...")
        train_s = compute_strength_features(hist, hist)
        train_s = compute_rest_days(train_s)
        upcoming_s = compute_strength_features(hist, upcoming)
        upcoming_s = compute_rest_days(upcoming_s)

        # Assign clusters
        train_s["cluster"] = assign_clusters_seasonal(train_s, train_s)
        upcoming_s["cluster"] = assign_clusters_seasonal(train_s, upcoming_s)

        results = []
        for line in [1.5, 2.5, 3.5]:
            # ── OVER MODEL: per-cluster Poisson ──
            clusters = sorted(train_s["cluster"].unique())
            over_probs = np.full(len(upcoming_s), np.nan)

            for cl in clusters:
                tr_cl = train_s[train_s["cluster"] == cl]
                up_cl = upcoming_s[upcoming_s["cluster"] == cl]
                if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(up_cl) == 0:
                    continue

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    params = _fit_poisson_lambda(tr_cl["total_goals"].values,
                                                tr_cl[OVER_COLS].values)
                idx = up_cl.index
                probs = _predict_poisson_over(params, up_cl[OVER_COLS].values, line)
                over_probs[upcoming_s.index.get_indexer(idx)] = probs

            nan_mask = np.isnan(over_probs)
            if nan_mask.any():
                over_probs[nan_mask] = (train_s["total_goals"] > line).mean()

            # ── UNDER MODEL: global Poisson ──
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params_u = _fit_poisson_lambda(train_s["total_goals"].values,
                                               train_s[UNDER_COLS].values)
            under_probs = 1.0 - _predict_poisson_over(
                params_u, upcoming_s[UNDER_COLS].values, line)

            for i in range(len(upcoming_s)):
                row = upcoming_s.iloc[i]
                over_p = float(over_probs[i])
                under_p = float(under_probs[i])
                league = row["league"]

                over_cal = _calibrate_prob(over_p, league)
                under_cal = _calibrate_prob(under_p, league)

                lg_avg = round(league_goal_avg.get(league, float("nan")), 2)

                if over_cal >= CALIBRATED_THRESHOLD:
                    results.append({
                        "fixture_id": int(row["fixture_id"]),
                        "kickoff": row["kickoff_utc"],
                        "home": row["home_team"],
                        "away": row["away_team"],
                        "league": league,
                        "line": line,
                        "direction": "OVER",
                        "probability": round(over_cal, 3),
                        "raw_probability": round(over_p, 3),
                        "home_elo": round(row["home_elo"], 0),
                        "away_elo": round(row["away_elo"], 0),
                        "cluster": int(row["cluster"]),
                        "league_goal_avg": lg_avg,
                    })
                if under_cal >= CALIBRATED_THRESHOLD:
                    results.append({
                        "fixture_id": int(row["fixture_id"]),
                        "kickoff": row["kickoff_utc"],
                        "home": row["home_team"],
                        "away": row["away_team"],
                        "league": league,
                        "line": line,
                        "direction": "UNDER",
                        "probability": round(under_cal, 3),
                        "raw_probability": round(under_p, 3),
                        "home_elo": round(row["home_elo"], 0),
                        "away_elo": round(row["away_elo"], 0),
                        "cluster": int(row["cluster"]),
                        "league_goal_avg": lg_avg,
                    })

        picks = pd.DataFrame(results)
        if picks.empty:
            print("\nNo picks above threshold.")
            return picks

        picks = picks.sort_values(["line", "direction", "probability"],
                                  ascending=[True, True, False])
        return picks
    finally:
        conn.close()


def _tier(prob: float) -> str:
    if prob >= 0.75:
        return "A"
    if prob >= 0.70:
        return "B"
    return "C"


def print_picks(picks: pd.DataFrame, threshold: float = None) -> None:
    if picks.empty:
        print("No picks.")
        return

    print(f"\n{'='*110}")
    print(f"  UPCOMING PICKS  (calibrated threshold >= {CALIBRATED_THRESHOLD:.0%})")
    print(f"  Tiers: A (>=75%, ~79% hit) | B (70-74%, ~76% hit) | C (65-69%, ~74% hit)")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*110}")

    for line in sorted(picks["line"].unique()):
        for direction in ["OVER", "UNDER"]:
            subset = picks[(picks["line"] == line) & (picks["direction"] == direction)]
            if subset.empty:
                continue

            avg_conf = subset["probability"].mean()
            tier_counts = subset["probability"].apply(_tier).value_counts()
            tier_str = "  ".join(f"{t}:{n}" for t, n in sorted(tier_counts.items()))
            print(f"\n── {direction} {line}  |  {len(subset)} picks  |  "
                  f"avg confidence {avg_conf:.0%}  |  {tier_str} ──\n")
            print(f"  {'Tier':>4s}  {'Date':>16s}  {'Home':>25s}    {'Away':<25s}  {'Conf':>5s}  {'Lg Avg':>6s}  League")
            print(f"  {'─'*4}  {'─'*16}  {'─'*25}    {'─'*25}  {'─'*5}  {'─'*6}  {'─'*20}")
            for _, row in subset.iterrows():
                dt = pd.Timestamp(row["kickoff"]).strftime("%a %b %d %H:%M")
                tier = _tier(row["probability"])
                lg_avg = row.get("league_goal_avg", float("nan"))
                lg_str = f"{lg_avg:.1f}" if pd.notna(lg_avg) else "  -"
                print(f"  {tier:>4s}  {dt:>16s}  {row['home']:>25s} vs {row['away']:<25s}  "
                      f"{row['probability']:>4.0%}  {lg_str:>6s}  {row['league']}")


def save_picks_yaml(picks: pd.DataFrame) -> str:
    import yaml
    from pathlib import Path

    ds = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = Path(__file__).parent / f"picks_{ds}.yaml"

    data = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "calibrated_threshold": CALIBRATED_THRESHOLD,
        "tiers": {"A": ">=75% (~79% hit)", "B": "70-74% (~76% hit)", "C": "65-69% (~74% hit)"},
        "picks": [],
    }

    for _, row in picks.iterrows():
        data["picks"].append({
            "fixture": f"{row['home']} vs {row['away']}",
            "kickoff": pd.Timestamp(row["kickoff"]).strftime("%Y-%m-%d %H:%M UTC"),
            "league": row["league"],
            "league_goal_avg": row.get("league_goal_avg", None),
            "line": row["line"],
            "direction": row["direction"],
            "confidence": row["probability"],
            "raw_confidence": row.get("raw_probability", None),
            "tier": _tier(row["probability"]),
        })

    with open(out_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"\nPicks saved to {out_path}")
    return str(out_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Predict upcoming over/under picks")
    parser.add_argument("--days", type=int, default=7, help="Days ahead to look")
    parser.add_argument("--threshold", type=float, default=0.60,
                        help="Minimum confidence (default: 0.60)")
    args = parser.parse_args()

    picks = predict_upcoming(days=args.days, threshold=args.threshold)
    print_picks(picks, args.threshold)
    if not picks.empty:
        save_picks_yaml(picks)
