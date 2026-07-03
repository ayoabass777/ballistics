"""
Per-cluster walk-forward backtest with class balancing.

Instead of one global model, trains a separate model per scoring-style
cluster.  Each cluster has a tighter distribution, so the model doesn't
just default to the majority class.

Two model types per cluster:
  A) Balanced logistic regression (class_weight='balanced')
  B) Poisson count model (derive over/under from fitted λ)

Compared against:
  C) Global balanced logistic regression (single model, all clusters)
  D) Global Poisson (single model, all clusters)

Usage:
    python -m engine.eval.cluster_backtest
    python -m engine.eval.cluster_backtest --csv DIR
"""

from __future__ import annotations

import argparse
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
import lightgbm as lgb
from scipy.optimize import minimize_scalar
from scipy.stats import poisson as poisson_dist
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
)
from sklearn.preprocessing import StandardScaler
from statsmodels.discrete.discrete_model import NegativeBinomial

from engine.eval.elo_goals import build_analysis_df, cluster_leagues
from engine.eval.league_clustering import build_league_profiles, cluster_ward
from engine.eval.team_strength import compute_team_strength, load_team_df_from_matches
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger

logger = get_logger(__name__)

# ── config ──────────────────────────────────────────────────────────────────
SOURCE_TABLE = "raw.fact_head_to_head"
ELO_TABLE = "elo.team_elo_history"

FOLD_MONTHS: int = 3
MIN_TRAIN_PER_CLUSTER: int = 100
MIN_TRAIN_TOTAL: int = 500
LINES: List[float] = [1.5, 2.5, 3.5]
ELO_COLS = ["abs_gap", "max_elo", "min_elo", "elo_sum", "home_elo_gap"]
FORM_WINDOW: int = 6
FORM_COLS = [
    "home_scored_avg", "home_conceded_avg",
    "away_scored_avg", "away_conceded_avg",
    "home_total_avg", "away_total_avg",
]
VOL_COLS = [
    "home_total_std", "away_total_std", "combined_vol",
]
ELO_PLUS_FORM_COLS = ELO_COLS + FORM_COLS
ELO_FORM_VOL_COLS = ELO_COLS + FORM_COLS + VOL_COLS
STRENGTH_COLS = [
    "home_attack_shrunk", "home_defense_shrunk",
    "away_attack_shrunk", "away_defense_shrunk",
]
ELO_STRENGTH_COLS = ELO_COLS + STRENGTH_COLS
INTERACT_COLS = [
    "home_atk_x_away_def", "away_atk_x_home_def",
]
ELO_STRENGTH_INTERACT_COLS = ELO_STRENGTH_COLS + INTERACT_COLS
HOME_GOAL_COLS = ["home_elo", "away_elo", "home_scored_avg", "away_conceded_avg"]
AWAY_GOAL_COLS = ["away_elo", "home_elo", "away_scored_avg", "home_conceded_avg"]
LGBM_COLS = ELO_COLS + FORM_COLS + VOL_COLS
PERCENTILE_COLS = [
    "home_attack_pct", "home_defense_pct",
    "away_attack_pct", "away_defense_pct",
]
REST_COLS = ["home_rest_days", "away_rest_days"]
ADAPTIVE_COLS = ["adaptive_gap", "adaptive_home_gap"]
FULL_COLS = ELO_COLS + STRENGTH_COLS + PERCENTILE_COLS + REST_COLS

ELO_GAP_SENSITIVITY = {
    "Virsliga": 0.267,
    "Premiership": 0.177,
    "Eredivisie": 0.168,
    "Primeira Liga": 0.142,
    "Süper Lig": 0.138,
    "Bundesliga": 0.122,
    "Eliteserien": 0.120,
    "Superliga": 0.113,
    "La Liga": 0.101,
    "USL League One": 0.088,
    "Ligue 1": 0.085,
    "First Division": 0.079,
    "Meistriliiga": 0.079,
    "Premier League": 0.079,
    "Premier Division": 0.078,
    "Jupiler Pro League": 0.069,
    "USL Championship": 0.066,
    "Serie B": 0.064,
    "Primera División": 0.062,
    "A Lyga": 0.057,
    "Erovnuli Liga": 0.052,
    "1. Deild": 0.048,
    "Serie A": 0.039,
    "1. Division": 0.037,
    "Allsvenskan": 0.033,
    "2. Bundesliga": 0.027,
    "Segunda Division": 0.027,
    "1 Lyga": 0.025,
    "Championship": 0.018,
    "Veikkausliiga": -0.049,
}
_MEDIAN_SENSITIVITY = 0.066

UNDER_COLS = [
    "home_attack_shrunk", "away_attack_shrunk",
    "home_defense_shrunk", "away_defense_shrunk",
    "home_attack_pct", "away_attack_pct",
    "home_defense_pct", "away_defense_pct",
    "abs_gap", "home_elo_gap",
    "home_rest_days", "away_rest_days",
]

OVER_COLS = ELO_COLS + STRENGTH_COLS + ["elo_gap_scaled"]
UNDER_COLS_TUNED = ELO_COLS + STRENGTH_COLS + REST_COLS + ["elo_gap_scaled"]

OVER_THRESHOLD: float = 0.70
UNDER_THRESHOLD: float = 0.70

CALIBRATION_BINS: List[Tuple[float, float]] = [
    (0.0, 0.2), (0.2, 0.4), (0.4, 0.5), (0.5, 0.6),
    (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0),
]


# ── data ────────────────────────────────────────────────────────────────────

def load_backtest_df(conn) -> pd.DataFrame:
    sql = f"""
        SELECT
            f.fixture_id, f.kickoff_utc, f.season,
            f.league_name AS league,
            f.team_id AS home_team_id, f.opponent_id AS away_team_id,
            f.goals_for AS home_goals, f.goals_against AS away_goals,
            COALESCE(eh.rating_before_flat, eh.rating_before) AS home_elo,
            COALESCE(ea.rating_before_flat, ea.rating_before) AS away_elo,
            eh.rating_before AS home_elo_adaptive,
            ea.rating_before AS away_elo_adaptive
        FROM {SOURCE_TABLE} f
        JOIN {ELO_TABLE} eh ON eh.team_id = f.team_id AND eh.fixture_id = f.fixture_id
        JOIN {ELO_TABLE} ea ON ea.team_id = f.opponent_id AND ea.fixture_id = f.fixture_id
        WHERE f.is_played AND f.is_home
        ORDER BY f.kickoff_utc
    """
    df = pd.read_sql(sql, conn)
    if df.empty:
        raise RuntimeError("No matches found — has elo_service been run?")
    return df


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["kickoff_utc"] = pd.to_datetime(df["kickoff_utc"], utc=True)
    # Flat-K Elo features (wider spreads, better for goal totals)
    df["abs_gap"] = (df["home_elo"] - df["away_elo"]).abs()
    df["max_elo"] = df[["home_elo", "away_elo"]].max(axis=1)
    df["min_elo"] = df[["home_elo", "away_elo"]].min(axis=1)
    df["elo_sum"] = df["home_elo"] + df["away_elo"]
    df["home_elo_gap"] = df["home_elo"] - df["away_elo"]
    # Adaptive-K Elo features (compressed in volatile leagues)
    if "home_elo_adaptive" in df.columns:
        df["adaptive_gap"] = (df["home_elo_adaptive"] - df["away_elo_adaptive"]).abs()
        df["adaptive_home_gap"] = df["home_elo_adaptive"] - df["away_elo_adaptive"]
    df["total_goals"] = df["home_goals"] + df["away_goals"]
    sensitivity = df["league"].map(ELO_GAP_SENSITIVITY).fillna(_MEDIAN_SENSITIVITY)
    df["elo_gap_scaled"] = df["abs_gap"] * sensitivity
    return df.sort_values("kickoff_utc").reset_index(drop=True)


def compute_form_features(df: pd.DataFrame, window: int = FORM_WINDOW) -> pd.DataFrame:
    """Rolling average goals scored/conceded per team over last `window` matches.

    Builds a team-level match log (both home and away appearances), computes
    rolling means strictly from *prior* matches (shift(1) prevents leakage),
    then joins back onto the fixture-level DataFrame.
    """
    df = df.copy()

    home = df[["fixture_id", "kickoff_utc", "home_team_id", "home_goals", "away_goals"]].rename(
        columns={"home_team_id": "team_id", "home_goals": "scored", "away_goals": "conceded"})
    away = df[["fixture_id", "kickoff_utc", "away_team_id", "away_goals", "home_goals"]].rename(
        columns={"away_team_id": "team_id", "away_goals": "scored", "home_goals": "conceded"})
    log = pd.concat([home, away]).sort_values("kickoff_utc")
    log["total"] = log["scored"] + log["conceded"]

    shifted = log.groupby("team_id")[["scored", "conceded", "total"]].apply(
        lambda g: g.shift(1), include_groups=False)
    rolled_mean = shifted.rolling(window, min_periods=1).mean()
    rolled_std = shifted[["total"]].rolling(window, min_periods=2).std()

    log[["r_scored", "r_conceded", "r_total"]] = rolled_mean.values
    log["r_total_std"] = rolled_std.values

    team_form = log[["fixture_id", "team_id", "r_scored", "r_conceded",
                     "r_total", "r_total_std"]]

    df = df.merge(
        team_form.rename(columns={
            "team_id": "home_team_id",
            "r_scored": "home_scored_avg",
            "r_conceded": "home_conceded_avg",
            "r_total": "home_total_avg",
            "r_total_std": "home_total_std",
        }),
        on=["fixture_id", "home_team_id"], how="left",
    )
    df = df.merge(
        team_form.rename(columns={
            "team_id": "away_team_id",
            "r_scored": "away_scored_avg",
            "r_conceded": "away_conceded_avg",
            "r_total": "away_total_avg",
            "r_total_std": "away_total_std",
        }),
        on=["fixture_id", "away_team_id"], how="left",
    )

    for col in FORM_COLS:
        df[col] = df[col].fillna(df["total_goals"].expanding().mean())

    global_std = df["total_goals"].expanding().std().bfill()
    for col in ("home_total_std", "away_total_std"):
        df[col] = df[col].fillna(global_std)

    df["combined_vol"] = (df["home_total_std"] + df["away_total_std"]) / 2.0

    return df


def compute_rest_days(df: pd.DataFrame) -> pd.DataFrame:
    """Add home_rest_days and away_rest_days columns.

    For each fixture, calculates the number of days since each team's
    previous match. Uses all prior rows in df (no train/test split needed
    since we only look backward).
    """
    out = df.copy()

    home = df[["fixture_id", "kickoff_utc", "home_team_id"]].rename(
        columns={"home_team_id": "team_id"})
    away = df[["fixture_id", "kickoff_utc", "away_team_id"]].rename(
        columns={"away_team_id": "team_id"})
    log = pd.concat([home, away]).sort_values("kickoff_utc")

    log["prev_kickoff"] = log.groupby("team_id")["kickoff_utc"].shift(1)
    log["rest_days"] = (
        log["kickoff_utc"] - log["prev_kickoff"]
    ).dt.total_seconds() / 86400.0

    median_rest = log["rest_days"].median()
    log["rest_days"] = log["rest_days"].fillna(median_rest).clip(upper=30.0)

    rest = log[["fixture_id", "team_id", "rest_days"]]

    out = out.merge(
        rest.rename(columns={"team_id": "home_team_id",
                             "rest_days": "home_rest_days"}),
        on=["fixture_id", "home_team_id"], how="left")
    out = out.merge(
        rest.rename(columns={"team_id": "away_team_id",
                             "rest_days": "away_rest_days"}),
        on=["fixture_id", "away_team_id"], how="left")

    for col in REST_COLS:
        out[col] = out[col].fillna(median_rest)

    return out


def compute_strength_features(train_df: pd.DataFrame,
                              target_df: pd.DataFrame) -> pd.DataFrame:
    """Compute team attack/defense shrunk rates from training data, join onto target.

    Uses only training matches to compute per-team shrunk rates via
    empirical-Bayes, then joins home/away team rates onto each fixture
    in target_df.
    """
    team_rows = load_team_df_from_matches(train_df)
    strength = compute_team_strength(team_rows)

    home_cols = strength[["league", "team_id", "attack_shrunk", "defense_shrunk",
                          "attack_percentile", "defense_percentile"]].rename(
        columns={"team_id": "home_team_id",
                 "attack_shrunk": "home_attack_shrunk",
                 "defense_shrunk": "home_defense_shrunk",
                 "attack_percentile": "home_attack_pct",
                 "defense_percentile": "home_defense_pct"})
    away_cols = strength[["league", "team_id", "attack_shrunk", "defense_shrunk",
                          "attack_percentile", "defense_percentile"]].rename(
        columns={"team_id": "away_team_id",
                 "attack_shrunk": "away_attack_shrunk",
                 "defense_shrunk": "away_defense_shrunk",
                 "attack_percentile": "away_attack_pct",
                 "defense_percentile": "away_defense_pct"})

    out = target_df.copy()
    out = out.merge(home_cols, on=["league", "home_team_id"], how="left")
    out = out.merge(away_cols, on=["league", "away_team_id"], how="left")

    # unseen teams in test get league-mean rates and 50th percentile
    league_means = team_rows.groupby("league").agg(
        mean_scored=("goals_for", "mean"),
        mean_conceded=("goals_against", "mean")).reset_index()
    for col, fallback_col in [("home_attack_shrunk", "mean_scored"),
                               ("home_defense_shrunk", "mean_conceded"),
                               ("away_attack_shrunk", "mean_scored"),
                               ("away_defense_shrunk", "mean_conceded")]:
        if out[col].isna().any():
            fill_map = dict(zip(league_means["league"], league_means[fallback_col]))
            global_mean = team_rows["goals_for"].mean()
            out[col] = out[col].fillna(out["league"].map(fill_map))
            out[col] = out[col].fillna(global_mean)

    for col in PERCENTILE_COLS:
        out[col] = out[col].fillna(50.0)

    out["home_atk_x_away_def"] = out["home_attack_shrunk"] * out["away_defense_shrunk"]
    out["away_atk_x_home_def"] = out["away_attack_shrunk"] * out["home_defense_shrunk"]

    return out


def assign_clusters(train_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.Series:
    adf = build_analysis_df(df=train_df[
        ["fixture_id", "league", "home_goals", "away_goals", "home_elo", "away_elo"]
    ])
    cluster_df, _ = cluster_leagues(adf)
    league_to_cluster = dict(zip(cluster_df["league"], cluster_df["cluster"]))
    fallback = cluster_df["cluster"].mode().iloc[0] if len(cluster_df) > 0 else 0
    return target_df["league"].map(lambda lg: league_to_cluster.get(lg, fallback))


def assign_clusters_hierarchical(train_df: pd.DataFrame,
                                  target_df: pd.DataFrame) -> pd.Series:
    """Assign leagues to clusters using Ward hierarchical clustering on training data."""
    profiles = build_league_profiles(train_df)
    if len(profiles) < 3:
        return pd.Series(1, index=target_df.index)
    ward_df, _, _ = cluster_ward(profiles)
    league_to_cluster = dict(zip(ward_df["league"], ward_df["cluster"]))
    fallback = ward_df["cluster"].mode().iloc[0]
    return target_df["league"].map(lambda lg: league_to_cluster.get(lg, fallback))


MIN_SEASON_MATCHES: int = 20


def assign_clusters_seasonal(train_df: pd.DataFrame,
                              target_df: pd.DataFrame) -> pd.Series:
    """Assign leagues to clusters using only the most recent season's data.

    Falls back to the previous season for leagues with too few matches
    in the latest season. Uses Ward hierarchical clustering on the
    per-season league profiles.
    """
    if "season" not in train_df.columns:
        return assign_clusters_hierarchical(train_df, target_df)

    latest_season = train_df["season"].max()
    season_df = train_df[train_df["season"] == latest_season]
    profiles = build_league_profiles(season_df)

    # leagues with too few matches in latest season: try previous season
    all_leagues = set(train_df["league"].unique())
    profiled_leagues = set(profiles["league"].unique()) if "league" in profiles.columns else set()
    missing = all_leagues - profiled_leagues

    if missing and latest_season > train_df["season"].min():
        prev_df = train_df[train_df["season"] == latest_season - 1]
        prev_profiles = build_league_profiles(prev_df)
        prev_fill = prev_profiles[prev_profiles["league"].isin(missing)]
        if not prev_fill.empty:
            profiles = pd.concat([profiles, prev_fill], ignore_index=True)

    if len(profiles) < 3:
        return pd.Series(1, index=target_df.index)

    ward_df, _, _ = cluster_ward(profiles)
    league_to_cluster = dict(zip(ward_df["league"], ward_df["cluster"]))
    fallback = ward_df["cluster"].mode().iloc[0]
    return target_df["league"].map(lambda lg: league_to_cluster.get(lg, fallback))


# ── model helpers ───────────────────────────────────────────────────────────

def _log_loss_safe(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    eps = 1e-15
    p = np.clip(y_prob, eps, 1 - eps)
    return -np.mean(y_true * np.log(p) + (1 - y_true) * np.log(1 - p))


def _metrics(y_true: np.ndarray, y_prob: np.ndarray,
             threshold: float = 0.5) -> Dict[str, float]:
    y_pred = (y_prob >= threshold).astype(int)
    return {
        "log_loss": _log_loss_safe(y_true, y_prob),
        "brier": float(np.mean((y_prob - y_true) ** 2)),
        "accuracy": float(np.mean(y_pred == y_true)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0.0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0.0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0.0)),
        "base_rate": float(y_true.mean()),
        "pred_positive_rate": float(y_pred.mean()),
    }


def _fit_poisson_lambda(y: np.ndarray, X: np.ndarray) -> np.ndarray:
    """Returns model params (including constant)."""
    X_c = sm.add_constant(X.astype(float))
    model = sm.GLM(y, X_c, family=sm.families.Poisson()).fit(disp=0)
    return model.params


def _predict_poisson_over(params: np.ndarray, X: np.ndarray,
                          line: float) -> np.ndarray:
    X_c = sm.add_constant(X.astype(float))
    lam = np.exp(X_c @ params)
    k = int(line)
    return 1.0 - poisson_dist.cdf(k, lam)


def _predict_split_over(params_home: np.ndarray, params_away: np.ndarray,
                        X_home: np.ndarray, X_away: np.ndarray,
                        line: float) -> np.ndarray:
    """P(home_goals + away_goals > line) from two independent Poissons."""
    X_h = sm.add_constant(X_home.astype(float))
    X_a = sm.add_constant(X_away.astype(float))
    lam_h = np.exp(X_h @ params_home)
    lam_a = np.exp(X_a @ params_away)
    k = int(line)
    n = len(lam_h)
    probs = np.zeros(n)
    for i in range(n):
        cum = 0.0
        for h in range(k + 1):
            cum += poisson_dist.pmf(h, lam_h[i]) * poisson_dist.cdf(k - h, lam_a[i])
        probs[i] = 1.0 - cum
    return probs


def _fit_negbin(y: np.ndarray, X: np.ndarray) -> Tuple[np.ndarray, float]:
    """Fit Negative Binomial model. Returns (params, alpha)."""
    X_c = sm.add_constant(X.astype(float))
    model = NegativeBinomial(y, X_c).fit(disp=0, maxiter=200)
    return model.params[:-1], np.exp(model.params[-1])


def _predict_negbin_over(params: np.ndarray, alpha: float,
                         X: np.ndarray, line: float) -> np.ndarray:
    """P(Y > line) under NegBin(mu, alpha) where mu = exp(X @ params)."""
    from scipy.stats import nbinom
    X_c = sm.add_constant(X.astype(float))
    mu = np.exp(X_c @ params)
    k = int(line)
    n_param = 1.0 / alpha
    p_param = n_param / (n_param + mu)
    return 1.0 - nbinom.cdf(k, n_param, p_param)


# ── Dixon-Coles bivariate Poisson ──────────────────────────────────────────

MAX_GOALS_DC: int = 10


def _dc_tau(x: int, y: int, lam: float, mu: float, rho: float) -> float:
    """Dixon-Coles correction factor for low-scoring outcomes."""
    if x == 0 and y == 0:
        return 1.0 - lam * mu * rho
    if x == 1 and y == 0:
        return 1.0 + mu * rho
    if x == 0 and y == 1:
        return 1.0 + lam * rho
    if x == 1 and y == 1:
        return 1.0 - rho
    return 1.0


def _dc_joint_pmf(x: int, y: int, lam: float, mu: float,
                  rho: float) -> float:
    """P(home=x, away=y) under Dixon-Coles model."""
    tau = _dc_tau(x, y, lam, mu, rho)
    return tau * poisson_dist.pmf(x, lam) * poisson_dist.pmf(y, mu)


def _dc_over_prob(lam: float, mu: float, rho: float,
                  line: float) -> float:
    """P(home + away > line) under Dixon-Coles."""
    k = int(line)
    p_under = 0.0
    for h in range(k + 1):
        for a in range(k - h + 1):
            p_under += _dc_joint_pmf(h, a, lam, mu, rho)
    return 1.0 - p_under


def _estimate_rho(home_goals: np.ndarray, away_goals: np.ndarray,
                  lam: np.ndarray, mu: np.ndarray) -> float:
    """MLE estimate of ρ given fitted λ, μ for each match."""
    def neg_ll(rho):
        ll = 0.0
        for i in range(len(home_goals)):
            hg, ag = int(home_goals[i]), int(away_goals[i])
            l, m = max(lam[i], 0.01), max(mu[i], 0.01)
            tau = _dc_tau(hg, ag, l, m, rho)
            if tau <= 0:
                return 1e12
            ll += np.log(tau)
        return -ll

    lam_max = lam.max()
    mu_max = mu.max()
    upper = min(1.0 / (lam_max * mu_max + 1e-8), 0.5)
    result = minimize_scalar(neg_ll, bounds=(-0.5, upper), method="bounded")
    return result.x


def _predict_dc_over(params_h: np.ndarray, params_a: np.ndarray,
                     X_home: np.ndarray, X_away: np.ndarray,
                     rho: float, line: float) -> np.ndarray:
    """P(total > line) for each match under Dixon-Coles."""
    X_h = sm.add_constant(X_home.astype(float))
    X_a = sm.add_constant(X_away.astype(float))
    lam = np.exp(X_h @ params_h)
    mu = np.exp(X_a @ params_a)
    n = len(lam)
    probs = np.zeros(n)
    for i in range(n):
        probs[i] = _dc_over_prob(lam[i], mu[i], rho, line)
    return probs


# ── LightGBM helpers ──────────────────────────────────────────────────────

LGB_PARAMS_REG = {
    "objective": "poisson",
    "metric": "poisson",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_child_samples": 50,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "verbose": -1,
    "n_jobs": 1,
    "seed": 42,
}

LGB_PARAMS_CLF = {
    "objective": "binary",
    "metric": "binary_logloss",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_child_samples": 50,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "verbose": -1,
    "n_jobs": 1,
    "seed": 42,
}


def _fit_lgbm_poisson(X_train: np.ndarray, y_train: np.ndarray,
                      feature_names: List[str]) -> lgb.Booster:
    ds = lgb.Dataset(X_train, label=y_train, feature_name=feature_names,
                     free_raw_data=False)
    return lgb.train(LGB_PARAMS_REG, ds, num_boost_round=200)


def _predict_lgbm_poisson_over(model: lgb.Booster, X: np.ndarray,
                               line: float) -> np.ndarray:
    lam = model.predict(X)
    k = int(line)
    return 1.0 - poisson_dist.cdf(k, lam)


def _fit_lgbm_clf(X_train: np.ndarray, y_train: np.ndarray,
                  feature_names: List[str]) -> lgb.Booster:
    ds = lgb.Dataset(X_train, label=y_train, feature_name=feature_names,
                     free_raw_data=False)
    return lgb.train(LGB_PARAMS_CLF, ds, num_boost_round=200)


# ── walk-forward engine ────────────────────────────────────────────────────

@dataclass
class FoldResult:
    fold: int
    test_start: str
    test_end: str
    n_train: int
    n_test: int
    cluster_n: Dict[int, int]


@dataclass
class BacktestResult:
    model_name: str
    folds: List[FoldResult] = field(default_factory=list)
    all_y_true: Dict[float, List[int]] = field(default_factory=dict)
    all_y_prob: Dict[float, List[float]] = field(default_factory=dict)
    all_clusters: List[int] = field(default_factory=list)
    all_fixture_ids: List[int] = field(default_factory=list)

    def init_lines(self):
        for line in LINES:
            self.all_y_true[line] = []
            self.all_y_prob[line] = []


def _walk_forward_splits(df: pd.DataFrame):
    ts = pd.to_datetime(df["kickoff_utc"])
    periods = ts.dt.tz_localize(None).dt.to_period("M")
    unique_months = periods.drop_duplicates().sort_values()

    i = 0
    fold_idx = 0
    while i + FOLD_MONTHS <= len(unique_months):
        test_months = unique_months[i: i + FOLD_MONTHS]
        test_start = test_months.iloc[0].start_time.tz_localize("UTC")
        test_end = test_months.iloc[-1].end_time.tz_localize("UTC")

        train_mask = df["kickoff_utc"] < test_start
        test_mask = (df["kickoff_utc"] >= test_start) & (df["kickoff_utc"] <= test_end)
        train = df[train_mask].copy()
        test = df[test_mask].copy()

        if len(train) < MIN_TRAIN_TOTAL or len(test) == 0:
            i += FOLD_MONTHS
            continue

        yield fold_idx, train, test, test_start, test_end
        fold_idx += 1
        i += FOLD_MONTHS


def run_per_cluster_balanced_lr(df: pd.DataFrame) -> BacktestResult:
    """Per-cluster logistic regression with class_weight='balanced'."""
    res = BacktestResult(model_name="per_cluster_balanced_lr")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters(train, train)
        test["cluster"] = assign_clusters(train, test)
        clusters = sorted(train["cluster"].unique())

        fold_probs = {line: np.full(len(test), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train[train["cluster"] == cl]
            te_cl = test[test["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr = tr_cl[ELO_COLS].values
            X_te = te_cl[ELO_COLS].values

            scaler = StandardScaler()
            X_tr_s = scaler.fit_transform(X_tr)
            X_te_s = scaler.transform(X_te)

            te_idx = te_cl.index

            for line in LINES:
                y_tr = (tr_cl["total_goals"] > line).astype(int).values
                if y_tr.sum() == 0 or y_tr.sum() == len(y_tr):
                    fold_probs[line][test.index.get_indexer(te_idx)] = y_tr.mean()
                    continue

                model = LogisticRegression(
                    class_weight="balanced", max_iter=1000, solver="lbfgs"
                )
                model.fit(X_tr_s, y_tr)
                probs = model.predict_proba(X_te_s)[:, 1]
                fold_probs[line][test.index.get_indexer(te_idx)] = probs

        # fill any unassigned (unseen clusters in test) with global base rate
        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test["cluster"].tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n=cluster_n,
        ))

    return res


def run_per_cluster_poisson(df: pd.DataFrame) -> BacktestResult:
    """Per-cluster Poisson count model."""
    res = BacktestResult(model_name="per_cluster_poisson")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters(train, train)
        test["cluster"] = assign_clusters(train, test)
        clusters = sorted(train["cluster"].unique())

        fold_probs = {line: np.full(len(test), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train[train["cluster"] == cl]
            te_cl = test[test["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr = tr_cl[ELO_COLS].values
            X_te = te_cl[ELO_COLS].values
            y_tr = tr_cl["total_goals"].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(y_tr, X_tr)

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_poisson_over(params, X_te, line)
                fold_probs[line][test.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test["cluster"].tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n=cluster_n,
        ))

    return res


def run_global_balanced_lr(df: pd.DataFrame) -> BacktestResult:
    """Global balanced logistic regression (baseline)."""
    res = BacktestResult(model_name="global_balanced_lr")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters(train, train)
        test["cluster"] = assign_clusters(train, test)

        cluster_dummies_tr = pd.get_dummies(train["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        cluster_dummies_te = pd.get_dummies(test["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        for c in cluster_dummies_tr.columns:
            if c not in cluster_dummies_te.columns:
                cluster_dummies_te[c] = 0.0
        cluster_dummies_te = cluster_dummies_te[cluster_dummies_tr.columns]
        if len(cluster_dummies_tr.columns) > 1:
            cluster_dummies_tr = cluster_dummies_tr.iloc[:, 1:]
            cluster_dummies_te = cluster_dummies_te.iloc[:, 1:]

        X_tr = np.hstack([train[ELO_COLS].values,
                          cluster_dummies_tr.values])
        X_te = np.hstack([test[ELO_COLS].values,
                          cluster_dummies_te.values])

        scaler = StandardScaler()
        X_tr_s = scaler.fit_transform(X_tr)
        X_te_s = scaler.transform(X_te)

        for line in LINES:
            y_tr = (train["total_goals"] > line).astype(int).values
            y_te = (test["total_goals"] > line).astype(int).values

            model = LogisticRegression(
                class_weight="balanced", max_iter=1000, solver="lbfgs"
            )
            model.fit(X_tr_s, y_tr)
            probs = model.predict_proba(X_te_s)[:, 1]

            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n={},
        ))

    return res


def run_global_poisson(df: pd.DataFrame) -> BacktestResult:
    """Global Poisson with cluster dummies (baseline)."""
    res = BacktestResult(model_name="global_poisson")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters(train, train)
        test["cluster"] = assign_clusters(train, test)

        cluster_dummies_tr = pd.get_dummies(train["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        cluster_dummies_te = pd.get_dummies(test["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        for c in cluster_dummies_tr.columns:
            if c not in cluster_dummies_te.columns:
                cluster_dummies_te[c] = 0.0
        cluster_dummies_te = cluster_dummies_te[cluster_dummies_tr.columns]
        if len(cluster_dummies_tr.columns) > 1:
            cluster_dummies_tr = cluster_dummies_tr.iloc[:, 1:]
            cluster_dummies_te = cluster_dummies_te.iloc[:, 1:]

        X_tr = np.hstack([train[ELO_COLS].values, cluster_dummies_tr.values])
        X_te = np.hstack([test[ELO_COLS].values, cluster_dummies_te.values])
        y_tr = train["total_goals"].values

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            params = _fit_poisson_lambda(y_tr, X_tr)

        for line in LINES:
            probs = _predict_poisson_over(params, X_te, line)
            y_te = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n={},
        ))

    return res


def run_global_poisson_form(df: pd.DataFrame) -> BacktestResult:
    """Global Poisson with Elo + form + cluster dummies."""
    res = BacktestResult(model_name="global_poisson_form")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters(train, train)
        test["cluster"] = assign_clusters(train, test)

        cluster_dummies_tr = pd.get_dummies(train["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        cluster_dummies_te = pd.get_dummies(test["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        for c in cluster_dummies_tr.columns:
            if c not in cluster_dummies_te.columns:
                cluster_dummies_te[c] = 0.0
        cluster_dummies_te = cluster_dummies_te[cluster_dummies_tr.columns]
        if len(cluster_dummies_tr.columns) > 1:
            cluster_dummies_tr = cluster_dummies_tr.iloc[:, 1:]
            cluster_dummies_te = cluster_dummies_te.iloc[:, 1:]

        X_tr = np.hstack([train[ELO_PLUS_FORM_COLS].values,
                          cluster_dummies_tr.values])
        X_te = np.hstack([test[ELO_PLUS_FORM_COLS].values,
                          cluster_dummies_te.values])
        y_tr = train["total_goals"].values

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            params = _fit_poisson_lambda(y_tr, X_tr)

        for line in LINES:
            probs = _predict_poisson_over(params, X_te, line)
            y_te = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n={},
        ))

    return res


def run_per_cluster_poisson_form(df: pd.DataFrame) -> BacktestResult:
    """Per-cluster Poisson with Elo + form features."""
    res = BacktestResult(model_name="per_cluster_poisson_form")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters(train, train)
        test["cluster"] = assign_clusters(train, test)
        clusters = sorted(train["cluster"].unique())

        fold_probs = {line: np.full(len(test), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train[train["cluster"] == cl]
            te_cl = test[test["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr = tr_cl[ELO_PLUS_FORM_COLS].values
            X_te = te_cl[ELO_PLUS_FORM_COLS].values
            y_tr = tr_cl["total_goals"].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(y_tr, X_tr)

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_poisson_over(params, X_te, line)
                fold_probs[line][test.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test["cluster"].tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n=cluster_n,
        ))

    return res


def run_global_poisson_vol(df: pd.DataFrame) -> BacktestResult:
    """Global Poisson with Elo + form + volatility + cluster dummies."""
    res = BacktestResult(model_name="global_poisson_vol")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters(train, train)
        test["cluster"] = assign_clusters(train, test)

        cluster_dummies_tr = pd.get_dummies(train["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        cluster_dummies_te = pd.get_dummies(test["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        for c in cluster_dummies_tr.columns:
            if c not in cluster_dummies_te.columns:
                cluster_dummies_te[c] = 0.0
        cluster_dummies_te = cluster_dummies_te[cluster_dummies_tr.columns]
        if len(cluster_dummies_tr.columns) > 1:
            cluster_dummies_tr = cluster_dummies_tr.iloc[:, 1:]
            cluster_dummies_te = cluster_dummies_te.iloc[:, 1:]

        X_tr = np.hstack([train[ELO_FORM_VOL_COLS].values,
                          cluster_dummies_tr.values])
        X_te = np.hstack([test[ELO_FORM_VOL_COLS].values,
                          cluster_dummies_te.values])
        y_tr = train["total_goals"].values

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            params = _fit_poisson_lambda(y_tr, X_tr)

        for line in LINES:
            probs = _predict_poisson_over(params, X_te, line)
            y_te = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n={},
        ))

    return res


def run_per_cluster_poisson_vol(df: pd.DataFrame) -> BacktestResult:
    """Per-cluster Poisson with Elo + form + volatility."""
    res = BacktestResult(model_name="per_cluster_poisson_vol")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters(train, train)
        test["cluster"] = assign_clusters(train, test)
        clusters = sorted(train["cluster"].unique())

        fold_probs = {line: np.full(len(test), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train[train["cluster"] == cl]
            te_cl = test[test["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr = tr_cl[ELO_FORM_VOL_COLS].values
            X_te = te_cl[ELO_FORM_VOL_COLS].values
            y_tr = tr_cl["total_goals"].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(y_tr, X_tr)

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_poisson_over(params, X_te, line)
                fold_probs[line][test.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test["cluster"].tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n=cluster_n,
        ))

    return res


def run_global_split_poisson(df: pd.DataFrame) -> BacktestResult:
    """Two independent Poissons: home goals + away goals, with form features."""
    res = BacktestResult(model_name="global_split_poisson")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        X_tr_h = train[HOME_GOAL_COLS].values
        X_tr_a = train[AWAY_GOAL_COLS].values
        X_te_h = test[HOME_GOAL_COLS].values
        X_te_a = test[AWAY_GOAL_COLS].values

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            params_h = _fit_poisson_lambda(train["home_goals"].values, X_tr_h)
            params_a = _fit_poisson_lambda(train["away_goals"].values, X_tr_a)

        for line in LINES:
            probs = _predict_split_over(params_h, params_a, X_te_h, X_te_a, line)
            y_te = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n={},
        ))

    return res


def run_per_cluster_split_poisson(df: pd.DataFrame) -> BacktestResult:
    """Per-cluster split home/away Poisson with form features."""
    res = BacktestResult(model_name="per_cluster_split_poisson")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters(train, train)
        test["cluster"] = assign_clusters(train, test)
        clusters = sorted(train["cluster"].unique())

        fold_probs = {line: np.full(len(test), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train[train["cluster"] == cl]
            te_cl = test[test["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr_h = tr_cl[HOME_GOAL_COLS].values
            X_tr_a = tr_cl[AWAY_GOAL_COLS].values
            X_te_h = te_cl[HOME_GOAL_COLS].values
            X_te_a = te_cl[AWAY_GOAL_COLS].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params_h = _fit_poisson_lambda(tr_cl["home_goals"].values, X_tr_h)
                params_a = _fit_poisson_lambda(tr_cl["away_goals"].values, X_tr_a)

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_split_over(params_h, params_a, X_te_h, X_te_a, line)
                fold_probs[line][test.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test["cluster"].tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n=cluster_n,
        ))

    return res


def run_global_dixon_coles(df: pd.DataFrame) -> BacktestResult:
    """Dixon-Coles bivariate Poisson with ρ correction for low scores."""
    res = BacktestResult(model_name="global_dixon_coles")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        X_tr_h = train[HOME_GOAL_COLS].values
        X_tr_a = train[AWAY_GOAL_COLS].values
        X_te_h = test[HOME_GOAL_COLS].values
        X_te_a = test[AWAY_GOAL_COLS].values

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            params_h = _fit_poisson_lambda(train["home_goals"].values, X_tr_h)
            params_a = _fit_poisson_lambda(train["away_goals"].values, X_tr_a)

        X_tr_h_c = sm.add_constant(X_tr_h.astype(float))
        X_tr_a_c = sm.add_constant(X_tr_a.astype(float))
        lam_tr = np.exp(X_tr_h_c @ params_h)
        mu_tr = np.exp(X_tr_a_c @ params_a)
        rho = _estimate_rho(
            train["home_goals"].values, train["away_goals"].values,
            lam_tr, mu_tr,
        )

        for line in LINES:
            probs = _predict_dc_over(params_h, params_a,
                                     X_te_h, X_te_a, rho, line)
            y_te = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n={},
        ))

    return res


def run_seasonal_cluster_dixon_coles(df: pd.DataFrame) -> BacktestResult:
    """Dixon-Coles per seasonal cluster."""
    res = BacktestResult(model_name="seasonal_cluster_dixon_coles")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters_seasonal(train, train)
        test["cluster"] = assign_clusters_seasonal(train, test)
        clusters = sorted(train["cluster"].unique())

        fold_probs = {line: np.full(len(test), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train[train["cluster"] == cl]
            te_cl = test[test["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr_h = tr_cl[HOME_GOAL_COLS].values
            X_tr_a = tr_cl[AWAY_GOAL_COLS].values
            X_te_h = te_cl[HOME_GOAL_COLS].values
            X_te_a = te_cl[AWAY_GOAL_COLS].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params_h = _fit_poisson_lambda(tr_cl["home_goals"].values, X_tr_h)
                params_a = _fit_poisson_lambda(tr_cl["away_goals"].values, X_tr_a)

            X_tr_h_c = sm.add_constant(X_tr_h.astype(float))
            X_tr_a_c = sm.add_constant(X_tr_a.astype(float))
            lam_tr = np.exp(X_tr_h_c @ params_h)
            mu_tr = np.exp(X_tr_a_c @ params_a)
            rho = _estimate_rho(
                tr_cl["home_goals"].values, tr_cl["away_goals"].values,
                lam_tr, mu_tr,
            )

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_dc_over(params_h, params_a,
                                         X_te_h, X_te_a, rho, line)
                fold_probs[line][test.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test["cluster"].tolist())
        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n=cluster_n,
        ))

    return res


def run_global_lgbm_poisson(df: pd.DataFrame) -> BacktestResult:
    """LightGBM Poisson regression on total goals, then CDF for over/under."""
    res = BacktestResult(model_name="global_lgbm_poisson")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        X_tr = train[LGBM_COLS].values
        X_te = test[LGBM_COLS].values
        y_tr = train["total_goals"].values

        model = _fit_lgbm_poisson(X_tr, y_tr, LGBM_COLS)

        for line in LINES:
            probs = _predict_lgbm_poisson_over(model, X_te, line)
            y_te = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n={},
        ))

    return res


def run_global_lgbm_classifier(df: pd.DataFrame) -> BacktestResult:
    """LightGBM binary classifier — one model per line, direct probability."""
    res = BacktestResult(model_name="global_lgbm_classifier")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        X_tr = train[LGBM_COLS].values
        X_te = test[LGBM_COLS].values

        for line in LINES:
            y_tr = (train["total_goals"].values > line).astype(int)
            y_te = (test["total_goals"].values > line).astype(int)
            model = _fit_lgbm_clf(X_tr, y_tr, LGBM_COLS)
            probs = model.predict(X_te)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n={},
        ))

    return res


def run_global_lgbm_strength(df: pd.DataFrame) -> BacktestResult:
    """LightGBM Poisson with team strength features added."""
    cols = LGBM_COLS + STRENGTH_COLS
    res = BacktestResult(model_name="global_lgbm_strength")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)
        X_tr = train_s[cols].values
        X_te = test_s[cols].values
        y_tr = train_s["total_goals"].values

        model = _fit_lgbm_poisson(X_tr, y_tr, cols)

        for line in LINES:
            probs = _predict_lgbm_poisson_over(model, X_te, line)
            y_te = (test_s["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test_s["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n={},
        ))

    return res


def run_seasonal_cluster_lgbm(df: pd.DataFrame) -> BacktestResult:
    """LightGBM Poisson per seasonal cluster."""
    res = BacktestResult(model_name="seasonal_cluster_lgbm")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters_seasonal(train, train)
        test["cluster"] = assign_clusters_seasonal(train, test)
        clusters = sorted(train["cluster"].unique())

        fold_probs = {line: np.full(len(test), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train[train["cluster"] == cl]
            te_cl = test[test["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr = tr_cl[LGBM_COLS].values
            X_te = te_cl[LGBM_COLS].values
            y_tr = tr_cl["total_goals"].values

            model = _fit_lgbm_poisson(X_tr, y_tr, LGBM_COLS)

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_lgbm_poisson_over(model, X_te, line)
                fold_probs[line][test.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test["cluster"].tolist())
        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n=cluster_n,
        ))

    return res


def run_global_poisson_full(df: pd.DataFrame) -> BacktestResult:
    """Global Poisson with Elo + strength + percentiles + rest days."""
    res = BacktestResult(model_name="global_poisson_full")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)
        train_s = compute_rest_days(train_s)
        test_s = compute_rest_days(test_s)

        X_tr = train_s[FULL_COLS].values
        X_te = test_s[FULL_COLS].values
        y_tr = train_s["total_goals"].values

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            params = _fit_poisson_lambda(y_tr, X_tr)

        for line in LINES:
            probs = _predict_poisson_over(params, X_te, line)
            y_te = (test_s["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test_s["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n={},
        ))

    return res


def run_seasonal_cluster_poisson_full(df: pd.DataFrame) -> BacktestResult:
    """Per-cluster Poisson with seasonal clustering + strength + percentiles + rest days."""
    res = BacktestResult(model_name="seasonal_cluster_poisson_full")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)
        train_s = compute_rest_days(train_s)
        test_s = compute_rest_days(test_s)

        train_s["cluster"] = assign_clusters_seasonal(train_s, train_s)
        test_s["cluster"] = assign_clusters_seasonal(train_s, test_s)
        clusters = sorted(train_s["cluster"].unique())

        fold_probs = {line: np.full(len(test_s), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train_s[train_s["cluster"] == cl]
            te_cl = test_s[test_s["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr = tr_cl[FULL_COLS].values
            X_te = te_cl[FULL_COLS].values
            y_tr = tr_cl["total_goals"].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(y_tr, X_tr)

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_poisson_over(params, X_te, line)
                fold_probs[line][test_s.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train_s["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test_s["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test_s["cluster"].tolist())
        res.all_fixture_ids.extend(test_s["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n=cluster_n,
        ))

    return res


def run_global_lgbm_full(df: pd.DataFrame) -> BacktestResult:
    """LightGBM Poisson with all features (Elo + form + vol + strength + pct + rest)."""
    cols = LGBM_COLS + STRENGTH_COLS + PERCENTILE_COLS + REST_COLS
    res = BacktestResult(model_name="global_lgbm_full")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)
        train_s = compute_rest_days(train_s)
        test_s = compute_rest_days(test_s)

        X_tr = train_s[cols].values
        X_te = test_s[cols].values
        y_tr = train_s["total_goals"].values

        model = _fit_lgbm_poisson(X_tr, y_tr, cols)

        for line in LINES:
            probs = _predict_lgbm_poisson_over(model, X_te, line)
            y_te = (test_s["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test_s["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n={},
        ))

    return res


def run_global_poisson_strength(df: pd.DataFrame) -> BacktestResult:
    """Global Poisson with Elo + team attack/defense shrunk rates + cluster dummies."""
    res = BacktestResult(model_name="global_poisson_strength")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)

        train_s["cluster"] = assign_clusters(train_s, train_s)
        test_s["cluster"] = assign_clusters(train_s, test_s)

        cluster_dummies_tr = pd.get_dummies(train_s["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        cluster_dummies_te = pd.get_dummies(test_s["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        for c in cluster_dummies_tr.columns:
            if c not in cluster_dummies_te.columns:
                cluster_dummies_te[c] = 0.0
        cluster_dummies_te = cluster_dummies_te[cluster_dummies_tr.columns]
        if len(cluster_dummies_tr.columns) > 1:
            cluster_dummies_tr = cluster_dummies_tr.iloc[:, 1:]
            cluster_dummies_te = cluster_dummies_te.iloc[:, 1:]

        X_tr = np.hstack([train_s[ELO_STRENGTH_COLS].values,
                          cluster_dummies_tr.values])
        X_te = np.hstack([test_s[ELO_STRENGTH_COLS].values,
                          cluster_dummies_te.values])
        y_tr = train_s["total_goals"].values

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            params = _fit_poisson_lambda(y_tr, X_tr)

        for line in LINES:
            probs = _predict_poisson_over(params, X_te, line)
            y_te = (test_s["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test_s["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train_s),
            n_test=len(test_s), cluster_n={},
        ))

    return res


def run_per_cluster_poisson_strength(df: pd.DataFrame) -> BacktestResult:
    """Per-cluster Poisson with Elo + team attack/defense shrunk rates."""
    res = BacktestResult(model_name="per_cluster_poisson_strength")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)

        train_s["cluster"] = assign_clusters(train_s, train_s)
        test_s["cluster"] = assign_clusters(train_s, test_s)
        clusters = sorted(train_s["cluster"].unique())

        fold_probs = {line: np.full(len(test_s), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train_s[train_s["cluster"] == cl]
            te_cl = test_s[test_s["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr = tr_cl[ELO_STRENGTH_COLS].values
            X_te = te_cl[ELO_STRENGTH_COLS].values
            y_tr = tr_cl["total_goals"].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(y_tr, X_tr)

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_poisson_over(params, X_te, line)
                fold_probs[line][test_s.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train_s["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test_s["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test_s["cluster"].tolist())

        res.all_fixture_ids.extend(test_s["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train_s),
            n_test=len(test_s), cluster_n=cluster_n,
        ))

    return res


def run_global_poisson_interact(df: pd.DataFrame) -> BacktestResult:
    """Global Poisson with Elo + strength + interaction features + cluster dummies."""
    res = BacktestResult(model_name="global_poisson_interact")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)

        train_s["cluster"] = assign_clusters(train_s, train_s)
        test_s["cluster"] = assign_clusters(train_s, test_s)

        cluster_dummies_tr = pd.get_dummies(train_s["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        cluster_dummies_te = pd.get_dummies(test_s["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        for c in cluster_dummies_tr.columns:
            if c not in cluster_dummies_te.columns:
                cluster_dummies_te[c] = 0.0
        cluster_dummies_te = cluster_dummies_te[cluster_dummies_tr.columns]
        if len(cluster_dummies_tr.columns) > 1:
            cluster_dummies_tr = cluster_dummies_tr.iloc[:, 1:]
            cluster_dummies_te = cluster_dummies_te.iloc[:, 1:]

        X_tr = np.hstack([train_s[ELO_STRENGTH_INTERACT_COLS].values,
                          cluster_dummies_tr.values])
        X_te = np.hstack([test_s[ELO_STRENGTH_INTERACT_COLS].values,
                          cluster_dummies_te.values])
        y_tr = train_s["total_goals"].values

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            params = _fit_poisson_lambda(y_tr, X_tr)

        for line in LINES:
            probs = _predict_poisson_over(params, X_te, line)
            y_te = (test_s["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test_s["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train_s),
            n_test=len(test_s), cluster_n={},
        ))

    return res


def run_global_negbin_strength(df: pd.DataFrame) -> BacktestResult:
    """Global Negative Binomial with Elo + strength + cluster dummies."""
    res = BacktestResult(model_name="global_negbin_strength")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)

        train_s["cluster"] = assign_clusters(train_s, train_s)
        test_s["cluster"] = assign_clusters(train_s, test_s)

        cluster_dummies_tr = pd.get_dummies(train_s["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        cluster_dummies_te = pd.get_dummies(test_s["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        for c in cluster_dummies_tr.columns:
            if c not in cluster_dummies_te.columns:
                cluster_dummies_te[c] = 0.0
        cluster_dummies_te = cluster_dummies_te[cluster_dummies_tr.columns]
        if len(cluster_dummies_tr.columns) > 1:
            cluster_dummies_tr = cluster_dummies_tr.iloc[:, 1:]
            cluster_dummies_te = cluster_dummies_te.iloc[:, 1:]

        X_tr = np.hstack([train_s[ELO_STRENGTH_COLS].values,
                          cluster_dummies_tr.values])
        X_te = np.hstack([test_s[ELO_STRENGTH_COLS].values,
                          cluster_dummies_te.values])
        y_tr = train_s["total_goals"].values

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                params, alpha = _fit_negbin(y_tr, X_tr)
            except Exception:
                params = _fit_poisson_lambda(y_tr, X_tr)
                alpha = None

        for line in LINES:
            if alpha is not None:
                probs = _predict_negbin_over(params, alpha, X_te, line)
            else:
                probs = _predict_poisson_over(params, X_te, line)
            y_te = (test_s["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test_s["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train_s),
            n_test=len(test_s), cluster_n={},
        ))

    return res


def run_global_negbin_interact(df: pd.DataFrame) -> BacktestResult:
    """Global Negative Binomial with Elo + strength + interactions + cluster dummies."""
    res = BacktestResult(model_name="global_negbin_interact")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)

        train_s["cluster"] = assign_clusters(train_s, train_s)
        test_s["cluster"] = assign_clusters(train_s, test_s)

        cluster_dummies_tr = pd.get_dummies(train_s["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        cluster_dummies_te = pd.get_dummies(test_s["cluster"].astype(str),
                                            prefix="cl", dtype=float)
        for c in cluster_dummies_tr.columns:
            if c not in cluster_dummies_te.columns:
                cluster_dummies_te[c] = 0.0
        cluster_dummies_te = cluster_dummies_te[cluster_dummies_tr.columns]
        if len(cluster_dummies_tr.columns) > 1:
            cluster_dummies_tr = cluster_dummies_tr.iloc[:, 1:]
            cluster_dummies_te = cluster_dummies_te.iloc[:, 1:]

        X_tr = np.hstack([train_s[ELO_STRENGTH_INTERACT_COLS].values,
                          cluster_dummies_tr.values])
        X_te = np.hstack([test_s[ELO_STRENGTH_INTERACT_COLS].values,
                          cluster_dummies_te.values])
        y_tr = train_s["total_goals"].values

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                params, alpha = _fit_negbin(y_tr, X_tr)
            except Exception:
                params = _fit_poisson_lambda(y_tr, X_tr)
                alpha = None

        for line in LINES:
            if alpha is not None:
                probs = _predict_negbin_over(params, alpha, X_te, line)
            else:
                probs = _predict_poisson_over(params, X_te, line)
            y_te = (test_s["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_te.tolist())
            res.all_y_prob[line].extend(probs.tolist())

        res.all_fixture_ids.extend(test_s["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train_s),
            n_test=len(test_s), cluster_n={},
        ))

    return res


def run_hier_cluster_poisson(df: pd.DataFrame) -> BacktestResult:
    """Per-cluster Poisson using Ward hierarchical clustering."""
    res = BacktestResult(model_name="hier_cluster_poisson")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters_hierarchical(train, train)
        test["cluster"] = assign_clusters_hierarchical(train, test)
        clusters = sorted(train["cluster"].unique())

        fold_probs = {line: np.full(len(test), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train[train["cluster"] == cl]
            te_cl = test[test["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr = tr_cl[ELO_COLS].values
            X_te = te_cl[ELO_COLS].values
            y_tr = tr_cl["total_goals"].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(y_tr, X_tr)

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_poisson_over(params, X_te, line)
                fold_probs[line][test.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test["cluster"].tolist())
        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n=cluster_n,
        ))

    return res


def run_hier_cluster_poisson_strength(df: pd.DataFrame) -> BacktestResult:
    """Per-cluster Poisson with Ward hierarchical clustering + team strength."""
    res = BacktestResult(model_name="hier_cluster_poisson_strength")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)

        train_s["cluster"] = assign_clusters_hierarchical(train_s, train_s)
        test_s["cluster"] = assign_clusters_hierarchical(train_s, test_s)
        clusters = sorted(train_s["cluster"].unique())

        fold_probs = {line: np.full(len(test_s), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train_s[train_s["cluster"] == cl]
            te_cl = test_s[test_s["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr = tr_cl[ELO_STRENGTH_COLS].values
            X_te = te_cl[ELO_STRENGTH_COLS].values
            y_tr = tr_cl["total_goals"].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(y_tr, X_tr)

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_poisson_over(params, X_te, line)
                fold_probs[line][test_s.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train_s["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test_s["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test_s["cluster"].tolist())
        res.all_fixture_ids.extend(test_s["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train_s),
            n_test=len(test_s), cluster_n=cluster_n,
        ))

    return res


def run_seasonal_cluster_poisson(df: pd.DataFrame) -> BacktestResult:
    """Per-cluster Poisson using seasonal Ward clustering."""
    res = BacktestResult(model_name="seasonal_cluster_poisson")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train["cluster"] = assign_clusters_seasonal(train, train)
        test["cluster"] = assign_clusters_seasonal(train, test)
        clusters = sorted(train["cluster"].unique())

        fold_probs = {line: np.full(len(test), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train[train["cluster"] == cl]
            te_cl = test[test["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr = tr_cl[ELO_COLS].values
            X_te = te_cl[ELO_COLS].values
            y_tr = tr_cl["total_goals"].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(y_tr, X_tr)

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_poisson_over(params, X_te, line)
                fold_probs[line][test.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test["cluster"].tolist())
        res.all_fixture_ids.extend(test["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train),
            n_test=len(test), cluster_n=cluster_n,
        ))

    return res


def run_seasonal_cluster_poisson_strength(df: pd.DataFrame) -> BacktestResult:
    """Per-cluster Poisson with seasonal Ward clustering + team strength."""
    res = BacktestResult(model_name="seasonal_cluster_poisson_strength")
    res.init_lines()

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)

        train_s["cluster"] = assign_clusters_seasonal(train_s, train_s)
        test_s["cluster"] = assign_clusters_seasonal(train_s, test_s)
        clusters = sorted(train_s["cluster"].unique())

        fold_probs = {line: np.full(len(test_s), np.nan) for line in LINES}
        cluster_n = {}

        for cl in clusters:
            tr_cl = train_s[train_s["cluster"] == cl]
            te_cl = test_s[test_s["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            cluster_n[cl] = len(te_cl)
            X_tr = tr_cl[ELO_STRENGTH_COLS].values
            X_te = te_cl[ELO_STRENGTH_COLS].values
            y_tr = tr_cl["total_goals"].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(y_tr, X_tr)

            te_idx = te_cl.index
            for line in LINES:
                probs = _predict_poisson_over(params, X_te, line)
                fold_probs[line][test_s.index.get_indexer(te_idx)] = probs

        for line in LINES:
            nan_mask = np.isnan(fold_probs[line])
            if nan_mask.any():
                fallback = (train_s["total_goals"] > line).mean()
                fold_probs[line][nan_mask] = fallback

            y_true = (test_s["total_goals"].values > line).astype(int)
            res.all_y_true[line].extend(y_true.tolist())
            res.all_y_prob[line].extend(fold_probs[line].tolist())

        res.all_clusters.extend(test_s["cluster"].tolist())
        res.all_fixture_ids.extend(test_s["fixture_id"].tolist())
        res.folds.append(FoldResult(
            fold=fold_idx, test_start=str(test_start.date()),
            test_end=str(test_end.date()), n_train=len(train_s),
            n_test=len(test_s), cluster_n=cluster_n,
        ))

    return res


# ── aggregation & reporting ─────────────────────────────────────────────────

def aggregate(res: BacktestResult) -> Dict:
    out = {"model": res.model_name, "n": len(res.all_y_true[LINES[0]])}
    for line in LINES:
        y = np.array(res.all_y_true[line])
        p = np.array(res.all_y_prob[line])
        out[f"over_{line}"] = _metrics(y, p)
    return out


def calibration_table(res: BacktestResult, line: float) -> pd.DataFrame:
    y = np.array(res.all_y_true[line])
    p = np.array(res.all_y_prob[line])
    rows = []
    for lo, hi in CALIBRATION_BINS:
        mask = (p >= lo) & (p < hi)
        n = mask.sum()
        if n == 0:
            continue
        rows.append({
            "prob_band": f"{lo:.1f}-{hi:.1f}",
            "n": int(n),
            "mean_pred": float(p[mask].mean()),
            "observed_rate": float(y[mask].mean()),
            "gap": float(y[mask].mean() - p[mask].mean()),
        })
    return pd.DataFrame(rows)


def per_cluster_detail(res: BacktestResult) -> pd.DataFrame:
    """Break down metrics by cluster for per-cluster models."""
    if not res.all_clusters:
        return pd.DataFrame()

    clusters = np.array(res.all_clusters)
    rows = []
    for cl in sorted(set(clusters)):
        mask = clusters == cl
        n = int(mask.sum())
        for line in LINES:
            y = np.array(res.all_y_true[line])[mask]
            p = np.array(res.all_y_prob[line])[mask]
            m = _metrics(y, p)
            rows.append({"cluster": cl, "line": line, "n": n, **m})
    return pd.DataFrame(rows)


def error_analysis(res: BacktestResult, df: pd.DataFrame,
                   line: float = 2.5) -> pd.DataFrame:
    """Build a match-level DataFrame with predictions, actuals, and features.

    Columns: fixture_id, league, home/away goals, total_goals, elo features,
    form features, cluster, predicted prob, actual outcome, correct flag,
    error type (false_over / false_under / correct_over / correct_under),
    confidence (distance from 0.5).
    """
    pred_df = pd.DataFrame({
        "fixture_id": res.all_fixture_ids,
        "pred_prob": res.all_y_prob[line],
        "actual": res.all_y_true[line],
    })
    if res.all_clusters:
        pred_df["cluster"] = res.all_clusters

    merge_cols = ["fixture_id", "league", "home_goals", "away_goals",
                  "total_goals", "home_elo", "away_elo", "abs_gap",
                  "home_elo_gap", "elo_sum"]
    form_present = [c for c in FORM_COLS if c in df.columns]
    vol_present = [c for c in VOL_COLS if c in df.columns]
    merge_cols += form_present + vol_present

    out = pred_df.merge(df[merge_cols].drop_duplicates("fixture_id"),
                        on="fixture_id", how="left")

    out["pred_over"] = (out["pred_prob"] >= 0.5).astype(int)
    out["correct"] = (out["pred_over"] == out["actual"]).astype(int)
    out["confidence"] = (out["pred_prob"] - 0.5).abs()

    conditions = [
        (out["pred_over"] == 1) & (out["actual"] == 0),
        (out["pred_over"] == 0) & (out["actual"] == 1),
        (out["pred_over"] == 1) & (out["actual"] == 1),
        (out["pred_over"] == 0) & (out["actual"] == 0),
    ]
    labels = ["false_over", "false_under", "correct_over", "correct_under"]
    out["error_type"] = np.select(conditions, labels, default="unknown")

    return out


def error_summary(analysis_df: pd.DataFrame) -> None:
    """Print a breakdown of model errors."""
    df = analysis_df
    n = len(df)
    print(f"  Total predictions: {n}")

    for et in ["correct_over", "correct_under", "false_over", "false_under"]:
        sub = df[df["error_type"] == et]
        pct = len(sub) / n * 100
        print(f"  {et:15s}: {len(sub):5d} ({pct:5.1f}%)")

    print()
    num_cols = ["abs_gap", "elo_sum", "home_elo_gap", "total_goals",
                "pred_prob", "confidence"]
    form_cols = [c for c in FORM_COLS if c in df.columns]
    vol_cols = [c for c in VOL_COLS if c in df.columns]
    num_cols += form_cols + vol_cols

    for et in ["false_over", "false_under"]:
        sub = df[df["error_type"] == et]
        correct_et = "correct_over" if et == "false_over" else "correct_under"
        correct_sub = df[df["error_type"] == correct_et]
        if sub.empty:
            continue
        print(f"  {et} vs {correct_et}:")
        for col in num_cols:
            if col not in df.columns:
                continue
            err_mean = sub[col].mean()
            cor_mean = correct_sub[col].mean() if not correct_sub.empty else float("nan")
            print(f"    {col:25s}  error={err_mean:7.2f}  correct={cor_mean:7.2f}  "
                  f"diff={err_mean - cor_mean:+7.2f}")
        print()

    # league breakdown of errors
    print("  Error rate by league (top 10 worst):")
    league_err = (df.groupby("league")
                  .agg(n=("correct", "count"),
                       errors=("correct", lambda x: (x == 0).sum()),
                       mean_prob=("pred_prob", "mean"),
                       actual_rate=("actual", "mean"))
                  .assign(error_rate=lambda x: x["errors"] / x["n"])
                  .sort_values("error_rate", ascending=False))
    with pd.option_context("display.max_rows", 10, "display.width", 200,
                           "display.float_format", "{:.3f}".format):
        print(league_err.head(10).to_string())
    print()

    # score distribution in errors
    print("  Score distribution in false_over (predicted over, actual under):")
    fo = df[df["error_type"] == "false_over"]
    if not fo.empty:
        score_dist = fo.groupby("total_goals").size().reset_index(name="count")
        score_dist["pct"] = score_dist["count"] / len(fo) * 100
        print(score_dist.to_string(index=False))
    print()

    print("  Score distribution in false_under (predicted under, actual over):")
    fu = df[df["error_type"] == "false_under"]
    if not fu.empty:
        score_dist = fu.groupby("total_goals").size().reset_index(name="count")
        score_dist["pct"] = score_dist["count"] / len(fu) * 100
        print(score_dist.to_string(index=False))


def confidence_filter_analysis(res: BacktestResult, line: float) -> pd.DataFrame:
    """Analyse hit rate at different confidence thresholds.

    For each threshold, shows how many games the model would bet on,
    the hit rate, and the edge over the base rate.
    """
    y_true = np.array(res.all_y_true[line])
    y_prob = np.array(res.all_y_prob[line])
    base_rate = y_true.mean()
    n_total = len(y_true)

    rows = []
    thresholds = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75]

    for thresh in thresholds:
        # over bets: model says P(over) > threshold
        over_mask = y_prob >= thresh
        n_over = over_mask.sum()
        if n_over > 0:
            hit_rate = y_true[over_mask].mean()
            rows.append({
                "direction": "over",
                "threshold": thresh,
                "n_bets": int(n_over),
                "pct_of_total": round(n_over / n_total * 100, 1),
                "hit_rate": round(hit_rate, 4),
                "base_rate": round(base_rate, 4),
                "edge": round(hit_rate - base_rate, 4),
            })

        # under bets: model says P(over) < (1 - threshold), i.e. P(under) > threshold
        under_thresh = 1.0 - thresh
        under_mask = y_prob <= under_thresh
        n_under = under_mask.sum()
        if n_under > 0:
            # hit rate for under = proportion that actually went under
            hit_rate = (1 - y_true[under_mask]).mean()
            rows.append({
                "direction": "under",
                "threshold": thresh,
                "n_bets": int(n_under),
                "pct_of_total": round(n_under / n_total * 100, 1),
                "hit_rate": round(hit_rate, 4),
                "base_rate": round(1.0 - base_rate, 4),
                "edge": round(hit_rate - (1.0 - base_rate), 4),
            })

    return pd.DataFrame(rows)


@dataclass
class StrategyResult:
    model_name: str
    fixture_ids: List[int] = field(default_factory=list)
    directions: List[str] = field(default_factory=list)
    probabilities: List[float] = field(default_factory=list)
    actuals: List[int] = field(default_factory=list)
    leagues: List[str] = field(default_factory=list)
    clusters: List[int] = field(default_factory=list)
    fold_labels: List[str] = field(default_factory=list)


def run_over_under_strategy(df: pd.DataFrame,
                            over_thresh: float = OVER_THRESHOLD,
                            under_thresh: float = UNDER_THRESHOLD,
                            ) -> StrategyResult:
    """Dedicated over/under strategy with separate models and confidence filters.

    Over model: seasonal-cluster Poisson on Elo + strength + rest features.
        Flags games where P(over 2.5) >= over_thresh.
    Under model: Poisson on attack/defense matchup profile.
        Flags games where P(under 2.5) >= under_thresh.
    """
    strat = StrategyResult(model_name="over_under_strategy")
    line = 2.5

    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)
        train_s = compute_rest_days(train_s)
        test_s = compute_rest_days(test_s)

        train_s["cluster"] = assign_clusters_seasonal(train_s, train_s)
        test_s["cluster"] = assign_clusters_seasonal(train_s, test_s)

        fold_label = f"{test_start.date()} → {test_end.date()}"

        # ── OVER MODEL: per-cluster Poisson on OVER_COLS ──
        clusters = sorted(train_s["cluster"].unique())
        over_probs = np.full(len(test_s), np.nan)

        for cl in clusters:
            tr_cl = train_s[train_s["cluster"] == cl]
            te_cl = test_s[test_s["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue

            X_tr = tr_cl[OVER_COLS].values
            X_te = te_cl[OVER_COLS].values

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(tr_cl["total_goals"].values, X_tr)

            te_idx = te_cl.index
            probs = _predict_poisson_over(params, X_te, line)
            over_probs[test_s.index.get_indexer(te_idx)] = probs

        nan_mask = np.isnan(over_probs)
        if nan_mask.any():
            over_probs[nan_mask] = (train_s["total_goals"] > line).mean()

        # ── UNDER MODEL: global Poisson on UNDER_COLS ──
        X_tr_u = train_s[UNDER_COLS].values
        X_te_u = test_s[UNDER_COLS].values

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            params_u = _fit_poisson_lambda(train_s["total_goals"].values, X_tr_u)

        under_probs_raw = _predict_poisson_over(params_u, X_te_u, line)
        under_probs = 1.0 - under_probs_raw

        y_true = (test_s["total_goals"].values > line).astype(int)

        # ── FLAG CONFIDENT PICKS ──
        for i in range(len(test_s)):
            row = test_s.iloc[i]
            fid = int(row["fixture_id"])
            lg = row["league"]
            cl = int(row["cluster"])

            if over_probs[i] >= over_thresh:
                strat.fixture_ids.append(fid)
                strat.directions.append("over")
                strat.probabilities.append(round(float(over_probs[i]), 4))
                strat.actuals.append(int(y_true[i]))
                strat.leagues.append(lg)
                strat.clusters.append(cl)
                strat.fold_labels.append(fold_label)

            elif under_probs[i] >= under_thresh:
                strat.fixture_ids.append(fid)
                strat.directions.append("under")
                strat.probabilities.append(round(float(under_probs[i]), 4))
                strat.actuals.append(int(y_true[i]))
                strat.leagues.append(lg)
                strat.clusters.append(cl)
                strat.fold_labels.append(fold_label)

    return strat


def strategy_report(strat: StrategyResult) -> pd.DataFrame:
    """Build summary report from strategy result."""
    if not strat.fixture_ids:
        return pd.DataFrame()

    df = pd.DataFrame({
        "fixture_id": strat.fixture_ids,
        "direction": strat.directions,
        "confidence": strat.probabilities,
        "actual_over": strat.actuals,
        "league": strat.leagues,
        "cluster": strat.clusters,
        "fold": strat.fold_labels,
    })

    df["hit"] = np.where(
        df["direction"] == "over",
        df["actual_over"] == 1,
        df["actual_over"] == 0,
    ).astype(int)

    return df


def print_strategy_report(strat: StrategyResult) -> None:
    """Print strategy performance summary."""
    df = strategy_report(strat)
    if df.empty:
        print("  No picks generated.")
        return

    n_total = len(df)
    n_folds = df["fold"].nunique()
    years = n_folds * 3 / 12

    print(f"  Total picks: {n_total} across {n_folds} folds (~{years:.1f} years)")
    print(f"  Per year: ~{n_total / years:.0f} picks")
    print()

    for direction in ["over", "under"]:
        sub = df[df["direction"] == direction]
        if sub.empty:
            continue

        n = len(sub)
        hits = sub["hit"].sum()
        hit_rate = hits / n
        avg_conf = sub["confidence"].mean()

        print(f"  {direction.upper()} picks:")
        print(f"    Count:       {n} ({n / n_total * 100:.1f}% of all picks)")
        print(f"    Hit rate:    {hit_rate:.3f} ({hits}/{n})")
        print(f"    Avg conf:    {avg_conf:.3f}")
        print(f"    Per year:    ~{n / years:.0f}")
        print()

        # by confidence band
        conf_bins = [(0.55, 0.60), (0.60, 0.65), (0.65, 0.70),
                     (0.70, 0.75), (0.75, 0.80), (0.80, 1.0)]
        print(f"    {'band':>10s}  {'n':>5s}  {'hit_rate':>8s}")
        for lo, hi in conf_bins:
            band = sub[(sub["confidence"] >= lo) & (sub["confidence"] < hi)]
            if len(band) > 0:
                print(f"    {lo:.2f}-{hi:.2f}  {len(band):5d}  {band['hit'].mean():8.3f}")
        print()

        # top 5 leagues by volume
        lg_perf = (sub.groupby("league")
                   .agg(n=("hit", "count"), hit_rate=("hit", "mean"),
                        avg_conf=("confidence", "mean"))
                   .sort_values("n", ascending=False))
        print(f"    Top leagues:")
        with pd.option_context("display.float_format", "{:.3f}".format):
            print(f"    {lg_perf.head(8).to_string()}")
        print()

    # combined
    print(f"  COMBINED:")
    print(f"    Total picks:  {n_total}")
    print(f"    Overall hit:  {df['hit'].mean():.3f} ({df['hit'].sum()}/{n_total})")
    print(f"    Per year:     ~{n_total / years:.0f}")


def _print_section(title: str) -> None:
    print(f"\n── {title} ──\n")


def run(csv_dir: Optional[str] = None, conn=None,
        df: Optional[pd.DataFrame] = None) -> Dict[str, BacktestResult]:
    close_conn = False
    if df is None:
        if conn is None:
            conn = get_db_connection()
            close_conn = True
        df = load_backtest_df(conn)

    df = prepare_features(df)
    df = compute_form_features(df)

    print(f"\n{'='*72}")
    print(f"  CLUSTER BACKTEST  ({len(df)} matches, form window={FORM_WINDOW})")
    print(f"  {df['kickoff_utc'].min().date()} → {df['kickoff_utc'].max().date()}")
    print(f"  {FOLD_MONTHS}-month folds, cluster assignment per fold")
    print(f"  Lines: {LINES}")
    print(f"{'='*72}")

    runners = {
        "global_poisson": run_global_poisson,
        "global_poisson_full": run_global_poisson_full,
        "global_lgbm_full": run_global_lgbm_full,
        "seasonal_cluster_poisson": run_seasonal_cluster_poisson,
        "seasonal_cluster_poisson_full": run_seasonal_cluster_poisson_full,
        "seasonal_cluster_dixon_coles": run_seasonal_cluster_dixon_coles,
    }

    results = {}
    for name, runner in runners.items():
        _print_section(name)
        res = runner(df)
        results[name] = res
        agg = aggregate(res)
        for line in LINES:
            m = agg[f"over_{line}"]
            print(f"  over {line}: ll={m['log_loss']:.4f}  "
                  f"brier={m['brier']:.4f}  acc={m['accuracy']:.3f}  "
                  f"prec={m['precision']:.3f}  rec={m['recall']:.3f}  "
                  f"f1={m['f1']:.3f}  base={m['base_rate']:.3f}  "
                  f"pred+={m['pred_positive_rate']:.3f}")

    # comparison table
    _print_section("Comparison — all models × all lines")
    comp_rows = []
    for name in runners:
        agg = aggregate(results[name])
        for line in LINES:
            m = agg[f"over_{line}"]
            comp_rows.append({"model": name, "line": line, **m})
    comp_df = pd.DataFrame(comp_rows)
    with pd.option_context("display.max_rows", None, "display.width", 200,
                           "display.float_format", "{:.4f}".format):
        print(comp_df.to_string(index=False))

    # calibration for best at each line
    for line in LINES:
        best = min(results, key=lambda k: aggregate(results[k])[f"over_{line}"]["log_loss"])
        _print_section(f"Calibration — over {line} ({best})")
        cal = calibration_table(results[best], line)
        with pd.option_context("display.float_format", "{:.3f}".format):
            print(cal.to_string(index=False))

    # per-cluster breakdown for per-cluster models
    for name in [k for k in results if "per_cluster" in k or "hier_cluster" in k or "seasonal_cluster" in k]:
        detail = per_cluster_detail(results[name])
        if not detail.empty:
            _print_section(f"Per-cluster breakdown — {name}")
            with pd.option_context("display.max_rows", None, "display.width", 200,
                                   "display.float_format", "{:.4f}".format):
                print(detail.to_string(index=False))

    # cluster distribution in a sample fold
    sample_res = results.get("per_cluster_poisson_vol",
                              results.get("per_cluster_poisson",
                              results.get("seasonal_cluster_poisson")))
    if sample_res and sample_res.folds:
        last_fold = sample_res.folds[-1]
        _print_section(f"Cluster sizes (last fold: {last_fold.test_start} → {last_fold.test_end})")
        for cl, n in sorted(last_fold.cluster_n.items()):
            print(f"  cluster {cl}: {n} test matches")

    # error analysis on best model at over 2.5
    best_model = min(
        results,
        key=lambda k: aggregate(results[k])["over_2.5"]["log_loss"],
    )
    _print_section(f"Error analysis — over 2.5 ({best_model})")
    ea = error_analysis(results[best_model], df, line=2.5)
    error_summary(ea)

    # confidence filter analysis — all models at over 2.5
    _print_section("Confidence filter — over 2.5 (all models, over direction)")
    cf_rows = []
    for name in results:
        cf = confidence_filter_analysis(results[name], 2.5)
        over_cf = cf[cf["direction"] == "over"].copy()
        over_cf.insert(0, "model", name)
        cf_rows.append(over_cf)
    cf_all = pd.concat(cf_rows, ignore_index=True)
    with pd.option_context("display.max_rows", None, "display.width", 220,
                           "display.float_format", "{:.4f}".format):
        print(cf_all.to_string(index=False))

    # under direction
    _print_section("Confidence filter — over 2.5 (all models, under direction)")
    cf_rows_u = []
    for name in results:
        cf = confidence_filter_analysis(results[name], 2.5)
        under_cf = cf[cf["direction"] == "under"].copy()
        under_cf.insert(0, "model", name)
        cf_rows_u.append(under_cf)
    cf_all_u = pd.concat(cf_rows_u, ignore_index=True)
    with pd.option_context("display.max_rows", None, "display.width", 220,
                           "display.float_format", "{:.4f}".format):
        print(cf_all_u.to_string(index=False))

    # ── Over/Under Strategy ──
    _print_section("OVER/UNDER STRATEGY (over 2.5)")
    print(f"  Over threshold:  {OVER_THRESHOLD:.0%} confidence")
    print(f"  Under threshold: {UNDER_THRESHOLD:.0%} confidence")
    print()
    strat = run_over_under_strategy(df)
    print_strategy_report(strat)

    if csv_dir:
        out = Path(csv_dir)
        out.mkdir(parents=True, exist_ok=True)
        comp_df.to_csv(out / "cluster_comparison.csv", index=False)
        for line in LINES:
            best = min(results, key=lambda k: aggregate(results[k])[f"over_{line}"]["log_loss"])
            cal = calibration_table(results[best], line)
            cal.to_csv(out / f"cluster_calibration_{line}.csv", index=False)
        for name in [k for k in results if "per_cluster" in k]:
            detail = per_cluster_detail(results[name])
            if not detail.empty:
                detail.to_csv(out / f"{name}_per_cluster.csv", index=False)
        ea.to_csv(out / "error_analysis_over_2_5.csv", index=False)
        strat_df = strategy_report(strat)
        if not strat_df.empty:
            strat_df.to_csv(out / "over_under_strategy.csv", index=False)
        print(f"\nCSVs written to {out}")

    if close_conn:
        conn.close()

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Per-cluster walk-forward backtest with class balancing."
    )
    parser.add_argument("--csv", metavar="DIR",
                        help="Write result CSVs to this directory.")
    args = parser.parse_args()
    run(csv_dir=args.csv)
