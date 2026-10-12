"""
Walk-forward backtest: can we classify a match as over 1.5 goals?

Approach
--------
Expanding-window walk-forward: train on all data before each test window,
predict the next window, slide forward.  No future leak.

Feature sets compared:
  A) elo_only         — abs_gap
  B) elo_rich         — abs_gap, max_elo, min_elo, elo_sum, home_elo_gap
  C) elo_plus_league  — elo_rich + per-league dummies
  D) elo_plus_cluster — elo_rich + scoring-style cluster dummies (from elo_goals)
  E) league_only      — per-league dummies (baseline)
  F) cluster_only     — cluster dummies (baseline)

The cluster assignment is fit on the TRAINING set each fold so it cannot
leak test-window information.

Usage:
    python -m engine.eval.over15_backtest
    python -m engine.eval.over15_backtest --csv DIR
    python -m engine.eval.over15_backtest --threshold 0.75
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
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

from engine.eval.elo_goals import (
    build_analysis_df,
    cluster_leagues,
    CLUSTER_FEATURES,
    MIN_RELIABLE_N,
)
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger

logger = get_logger(__name__)

# ── config ──────────────────────────────────────────────────────────────────
SOURCE_TABLE = "raw.engine_head_to_head"
ELO_TABLE = "elo.team_elo_history"

FOLD_MONTHS: int = 3
MIN_TRAIN_MATCHES: int = 500
DECISION_THRESHOLD: float = 0.5
CALIBRATION_BINS: List[Tuple[float, float]] = [
    (0.0, 0.5), (0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0),
]

ELO_RICH_COLS = ["abs_gap", "max_elo", "min_elo", "elo_sum", "home_elo_gap"]

FEATURE_SETS = [
    "elo_only",
    "elo_rich",
    "elo_plus_league",
    "elo_plus_cluster",
    "league_only",
    "cluster_only",
]


# ── data ────────────────────────────────────────────────────────────────────

def load_backtest_df(conn) -> pd.DataFrame:
    sql = f"""
        SELECT
            f.fixture_id,
            f.kickoff_utc,
            f.league_name                     AS league,
            f.goals_for                       AS home_goals,
            f.goals_against                   AS away_goals,
            eh.rating_before                  AS home_elo,
            ea.rating_before                  AS away_elo
        FROM {SOURCE_TABLE} f
        JOIN {ELO_TABLE} eh
          ON eh.team_id    = f.team_id
         AND eh.fixture_id = f.fixture_id
        JOIN {ELO_TABLE} ea
          ON ea.team_id    = f.opponent_id
         AND ea.fixture_id = f.fixture_id
        WHERE f.is_played
          AND f.is_home
        ORDER BY f.kickoff_utc
    """
    df = pd.read_sql(sql, conn)
    if df.empty:
        raise RuntimeError("No matches found — has elo_service been run?")
    return df


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["kickoff_utc"] = pd.to_datetime(df["kickoff_utc"], utc=True)
    df["abs_gap"] = (df["home_elo"] - df["away_elo"]).abs()
    df["max_elo"] = df[["home_elo", "away_elo"]].max(axis=1)
    df["min_elo"] = df[["home_elo", "away_elo"]].min(axis=1)
    df["elo_sum"] = df["home_elo"] + df["away_elo"]
    df["home_elo_gap"] = df["home_elo"] - df["away_elo"]
    df["total_goals"] = df["home_goals"] + df["away_goals"]
    df["over_1_5"] = (df["total_goals"] > 1.5).astype(int)
    return df.sort_values("kickoff_utc").reset_index(drop=True)


# ── cluster assignment (train-only, no leak) ────────────────────────────────

def _assign_clusters(train_df: pd.DataFrame, full_df: pd.DataFrame
                     ) -> pd.Series:
    """
    Fit clusters on training data, map every league in full_df to its cluster.
    Leagues unseen in training get the most common cluster.
    """
    adf = build_analysis_df(df=train_df[
        ["fixture_id", "league", "home_goals", "away_goals",
         "home_elo", "away_elo"]
    ])
    cluster_df, _ = cluster_leagues(adf)
    league_to_cluster = dict(zip(cluster_df["league"], cluster_df["cluster"]))
    fallback = cluster_df["cluster"].mode().iloc[0] if len(cluster_df) > 0 else 0
    return full_df["league"].map(lambda lg: league_to_cluster.get(lg, fallback))


# ── walk-forward engine ────────────────────────────────────────────────────

@dataclass
class FoldResult:
    fold: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    n_train: int
    n_test: int
    accuracy: float
    log_loss_val: float
    brier: float
    precision: float
    recall: float
    f1: float
    base_rate: float
    mean_pred_prob: float


@dataclass
class BacktestResult:
    feature_set: str
    folds: List[FoldResult] = field(default_factory=list)
    all_y_true: List[int] = field(default_factory=list)
    all_y_prob: List[float] = field(default_factory=list)
    all_y_pred: List[int] = field(default_factory=list)
    coef_history: List[Dict[str, float]] = field(default_factory=list)


def _build_X(df: pd.DataFrame, feature_set: str,
             all_leagues: List[str]) -> pd.DataFrame:
    # league dummies (consistent columns across folds)
    league_dummies = pd.get_dummies(df["league"], dtype=float)
    for lg in all_leagues:
        if lg not in league_dummies.columns:
            league_dummies[lg] = 0.0
    league_dummies = league_dummies[all_leagues]
    if len(all_leagues) > 1:
        league_dummies = league_dummies.iloc[:, 1:]

    # cluster dummies
    cluster_dummies = pd.get_dummies(
        df["cluster"].astype(str), prefix="cluster", dtype=float
    )
    all_cluster_cols = sorted(cluster_dummies.columns.tolist())
    for c in all_cluster_cols:
        if c not in cluster_dummies.columns:
            cluster_dummies[c] = 0.0
    if len(all_cluster_cols) > 1:
        cluster_dummies = cluster_dummies[all_cluster_cols[1:]]

    elo_rich = df[ELO_RICH_COLS].copy().reset_index(drop=True)
    ld = league_dummies.reset_index(drop=True)
    cd = cluster_dummies.reset_index(drop=True)

    if feature_set == "elo_only":
        return df[["abs_gap"]].copy().reset_index(drop=True)
    elif feature_set == "elo_rich":
        return elo_rich
    elif feature_set == "elo_plus_league":
        return pd.concat([elo_rich, ld], axis=1)
    elif feature_set == "elo_plus_cluster":
        return pd.concat([elo_rich, cd], axis=1)
    elif feature_set == "league_only":
        return ld
    elif feature_set == "cluster_only":
        return cd
    raise ValueError(f"Unknown feature set: {feature_set}")


def walk_forward(df: pd.DataFrame, feature_set: str,
                 threshold: float = DECISION_THRESHOLD) -> BacktestResult:
    result = BacktestResult(feature_set=feature_set)
    needs_cluster = feature_set in ("elo_plus_cluster", "cluster_only")

    all_leagues = sorted(df["league"].unique().tolist())
    ts = pd.to_datetime(df["kickoff_utc"])
    periods = ts.dt.tz_localize(None).dt.to_period("M")
    unique_months = periods.drop_duplicates().sort_values()

    fold_idx = 0
    i = 0
    while i + FOLD_MONTHS <= len(unique_months):
        test_months = unique_months[i: i + FOLD_MONTHS]
        test_start = test_months.iloc[0].start_time.tz_localize("UTC")
        test_end = test_months.iloc[-1].end_time.tz_localize("UTC")

        train_mask = df["kickoff_utc"] < test_start
        test_mask = (df["kickoff_utc"] >= test_start) & (df["kickoff_utc"] <= test_end)

        train = df[train_mask].copy()
        test = df[test_mask].copy()

        if len(train) < MIN_TRAIN_MATCHES or len(test) == 0:
            i += FOLD_MONTHS
            continue

        if needs_cluster:
            train["cluster"] = _assign_clusters(train, train)
            test["cluster"] = _assign_clusters(train, test)
        else:
            train["cluster"] = 0
            test["cluster"] = 0

        X_train = _build_X(train, feature_set, all_leagues)
        X_test = _build_X(test, feature_set, all_leagues)
        y_train = train["over_1_5"].values
        y_test = test["over_1_5"].values

        scaler = StandardScaler()
        X_train_s = scaler.fit_transform(X_train.values)
        X_test_s = scaler.transform(X_test.values)

        model = LogisticRegression(max_iter=1000, solver="lbfgs")
        model.fit(X_train_s, y_train)

        y_prob = model.predict_proba(X_test_s)[:, 1]
        y_pred = (y_prob >= threshold).astype(int)

        result.all_y_true.extend(y_test.tolist())
        result.all_y_prob.extend(y_prob.tolist())
        result.all_y_pred.extend(y_pred.tolist())

        coefs = dict(zip(X_train.columns, model.coef_[0]))
        result.coef_history.append(coefs)

        fr = FoldResult(
            fold=fold_idx,
            train_start=str(train["kickoff_utc"].min().date()),
            train_end=str(train["kickoff_utc"].max().date()),
            test_start=str(test_start.date()),
            test_end=str(test_end.date()),
            n_train=len(train),
            n_test=len(test),
            accuracy=accuracy_score(y_test, y_pred),
            log_loss_val=log_loss(y_test, y_prob),
            brier=brier_score_loss(y_test, y_prob),
            precision=precision_score(y_test, y_pred, zero_division=0.0),
            recall=recall_score(y_test, y_pred, zero_division=0.0),
            f1=f1_score(y_test, y_pred, zero_division=0.0),
            base_rate=y_test.mean(),
            mean_pred_prob=y_prob.mean(),
        )
        result.folds.append(fr)
        fold_idx += 1
        i += FOLD_MONTHS

    return result


# ── aggregate metrics ───────────────────────────────────────────────────────

def aggregate_metrics(res: BacktestResult, threshold: float = DECISION_THRESHOLD
                      ) -> Dict[str, float]:
    y_true = np.array(res.all_y_true)
    y_prob = np.array(res.all_y_prob)
    y_pred = np.array(res.all_y_pred)
    n = len(y_true)
    if n == 0:
        return {}
    return {
        "n": n,
        "base_rate": y_true.mean(),
        "accuracy": accuracy_score(y_true, y_pred),
        "log_loss": log_loss(y_true, y_prob),
        "brier": brier_score_loss(y_true, y_prob),
        "precision": precision_score(y_true, y_pred, zero_division=0.0),
        "recall": recall_score(y_true, y_pred, zero_division=0.0),
        "f1": f1_score(y_true, y_pred, zero_division=0.0),
        "threshold": threshold,
    }


def calibration_table(res: BacktestResult) -> pd.DataFrame:
    y_true = np.array(res.all_y_true)
    y_prob = np.array(res.all_y_prob)
    rows = []
    for lo, hi in CALIBRATION_BINS:
        mask = (y_prob >= lo) & (y_prob < hi)
        n = mask.sum()
        if n == 0:
            continue
        rows.append({
            "prob_band": f"{lo:.1f}-{hi:.1f}",
            "n": int(n),
            "mean_pred": y_prob[mask].mean(),
            "observed_rate": y_true[mask].mean(),
            "gap": y_true[mask].mean() - y_prob[mask].mean(),
        })
    return pd.DataFrame(rows)


def feature_importance(res: BacktestResult) -> pd.DataFrame:
    if not res.coef_history:
        return pd.DataFrame()
    all_feats = set()
    for d in res.coef_history:
        all_feats.update(d.keys())

    rows = []
    for feat in sorted(all_feats):
        vals = [d.get(feat, 0.0) for d in res.coef_history]
        rows.append({
            "feature": feat,
            "mean_coef": np.mean(vals),
            "std_coef": np.std(vals),
            "mean_abs_coef": np.mean(np.abs(vals)),
        })
    return (pd.DataFrame(rows)
            .sort_values("mean_abs_coef", ascending=False)
            .reset_index(drop=True))


# ── reporting ───────────────────────────────────────────────────────────────

def _print_section(title: str) -> None:
    print(f"\n── {title} ──\n")


def run(csv_dir: Optional[str] = None, threshold: float = DECISION_THRESHOLD,
        conn=None, df: Optional[pd.DataFrame] = None) -> Dict[str, BacktestResult]:
    close_conn = False
    if df is None:
        if conn is None:
            conn = get_db_connection()
            close_conn = True
        df = load_backtest_df(conn)

    df = prepare_features(df)

    print(f"\n{'='*72}")
    print(f"  OVER 1.5 WALK-FORWARD BACKTEST v2  ({len(df)} matches, "
          f"threshold={threshold})")
    print(f"  {df['kickoff_utc'].min().date()} → {df['kickoff_utc'].max().date()}")
    print(f"  {FOLD_MONTHS}-month folds, expanding train window")
    print(f"  Features: elo_rich = {ELO_RICH_COLS}")
    print(f"  Cluster assignment fitted per-fold on train data only")
    print(f"{'='*72}")

    results = {}
    for fs_name in FEATURE_SETS:
        _print_section(f"Feature set: {fs_name}")
        res = walk_forward(df, fs_name, threshold=threshold)
        results[fs_name] = res

        agg = aggregate_metrics(res, threshold)
        print(f"  n={agg['n']}  acc={agg['accuracy']:.3f}  "
              f"ll={agg['log_loss']:.4f}  brier={agg['brier']:.4f}  "
              f"prec={agg['precision']:.3f}  rec={agg['recall']:.3f}  "
              f"f1={agg['f1']:.3f}  base_rate={agg['base_rate']:.3f}")

    # best model detail
    best_name = min(results, key=lambda k: aggregate_metrics(results[k], threshold).get("log_loss", 999))
    best_res = results[best_name]

    _print_section(f"Best model detail: {best_name}")
    fold_rows = []
    for f in best_res.folds:
        fold_rows.append({
            "fold": f.fold,
            "test_window": f"{f.test_start} → {f.test_end}",
            "n_train": f.n_train, "n_test": f.n_test,
            "acc": f.accuracy, "ll": f.log_loss_val, "brier": f.brier,
            "prec": f.precision, "rec": f.recall, "f1": f.f1,
            "base": f.base_rate,
        })
    fold_df = pd.DataFrame(fold_rows)
    with pd.option_context("display.max_rows", None, "display.width", 200,
                           "display.float_format", "{:.3f}".format):
        print(fold_df.to_string(index=False))

    _print_section(f"Calibration ({best_name})")
    cal = calibration_table(best_res)
    with pd.option_context("display.float_format", "{:.3f}".format):
        print(cal.to_string(index=False))

    _print_section(f"Feature importance ({best_name})")
    fi = feature_importance(best_res)
    with pd.option_context("display.max_rows", 30, "display.float_format",
                           "{:.4f}".format):
        print(fi.to_string(index=False))

    _print_section("Model comparison summary")
    comp_rows = []
    for fs_name in FEATURE_SETS:
        agg = aggregate_metrics(results[fs_name], threshold)
        comp_rows.append({"feature_set": fs_name, **agg})
    comp_df = pd.DataFrame(comp_rows).sort_values("log_loss")
    with pd.option_context("display.float_format", "{:.4f}".format):
        print(comp_df.to_string(index=False))

    baseline_ll = aggregate_metrics(results["league_only"], threshold)["log_loss"]
    cluster_ll = aggregate_metrics(results["elo_plus_cluster"], threshold)["log_loss"]
    elo_league_ll = aggregate_metrics(results["elo_plus_league"], threshold)["log_loss"]
    print(f"\n  league_only baseline:  {baseline_ll:.4f}")
    print(f"  elo + league dummies:  {elo_league_ll:.4f}  "
          f"(delta={elo_league_ll - baseline_ll:+.4f})")
    print(f"  elo + cluster:         {cluster_ll:.4f}  "
          f"(delta={cluster_ll - baseline_ll:+.4f})")

    if csv_dir:
        out = Path(csv_dir)
        out.mkdir(parents=True, exist_ok=True)
        for fs_name, res in results.items():
            rows = [{"fold": f.fold, "test_start": f.test_start,
                      "test_end": f.test_end, "n_train": f.n_train,
                      "n_test": f.n_test, "accuracy": f.accuracy,
                      "log_loss": f.log_loss_val, "brier": f.brier,
                      "precision": f.precision, "recall": f.recall,
                      "f1": f.f1, "base_rate": f.base_rate}
                     for f in res.folds]
            pd.DataFrame(rows).to_csv(out / f"over15_folds_{fs_name}.csv",
                                      index=False)
        cal.to_csv(out / "over15_calibration.csv", index=False)
        fi.to_csv(out / "over15_feature_importance.csv", index=False)
        comp_df.to_csv(out / "over15_comparison.csv", index=False)
        print(f"\nCSVs written to {out}")

    if close_conn:
        conn.close()

    return results


# ── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Walk-forward backtest: over 1.5 goals classifier (v2)."
    )
    parser.add_argument("--csv", metavar="DIR",
                        help="Write result CSVs to this directory.")
    parser.add_argument("--threshold", type=float, default=DECISION_THRESHOLD,
                        help=f"Decision boundary (default {DECISION_THRESHOLD}).")
    args = parser.parse_args()
    run(csv_dir=args.csv, threshold=args.threshold)
