"""
Walk-forward backtest: Poisson & NegBin count models for total goals.

Instead of binary classification, model total goals as a count.  Then derive
over/under probabilities at any line (1.5, 2.5, 3.5) from the fitted
distribution — one model, no class-imbalance problem.

Feature sets compared:
  A) elo_rich              — abs_gap, max_elo, min_elo, elo_sum, home_elo_gap
  B) elo_plus_cluster      — elo_rich + scoring-style cluster dummies
  C) elo_plus_league       — elo_rich + per-league dummies
  D) cluster_only          — cluster dummies (baseline)
  E) league_only           — per-league dummies (baseline)

Each feature set is fitted as both Poisson and NegBin.  The better family
(by AIC on training data) is used for test-set evaluation.

Usage:
    python -m engine.eval.goals_backtest
    python -m engine.eval.goals_backtest --csv DIR
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
from scipy.stats import nbinom, poisson
from statsmodels.discrete.discrete_model import NegativeBinomial

from engine.eval.elo_goals import build_analysis_df, cluster_leagues
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger

logger = get_logger(__name__)

# ── config ──────────────────────────────────────────────────────────────────
SOURCE_TABLE = "raw.engine_head_to_head"
ELO_TABLE = "elo.team_elo_history"

FOLD_MONTHS: int = 3
MIN_TRAIN_MATCHES: int = 500

LINES: List[float] = [1.5, 2.5, 3.5]

ELO_RICH_COLS = ["abs_gap", "max_elo", "min_elo", "elo_sum", "home_elo_gap"]

FEATURE_SETS = [
    "elo_rich",
    "elo_plus_cluster",
    "elo_plus_league",
    "cluster_only",
    "league_only",
]

CALIBRATION_BINS: List[Tuple[float, float]] = [
    (0.0, 0.3), (0.3, 0.5), (0.5, 0.6), (0.6, 0.7),
    (0.7, 0.8), (0.8, 0.9), (0.9, 1.0),
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
    return df.sort_values("kickoff_utc").reset_index(drop=True)


def _assign_clusters(train_df: pd.DataFrame, full_df: pd.DataFrame
                     ) -> pd.Series:
    adf = build_analysis_df(df=train_df[
        ["fixture_id", "league", "home_goals", "away_goals",
         "home_elo", "away_elo"]
    ])
    cluster_df, _ = cluster_leagues(adf)
    league_to_cluster = dict(zip(cluster_df["league"], cluster_df["cluster"]))
    fallback = cluster_df["cluster"].mode().iloc[0] if len(cluster_df) > 0 else 0
    return full_df["league"].map(lambda lg: league_to_cluster.get(lg, fallback))


# ── feature matrix ──────────────────────────────────────────────────────────

def _build_X(df: pd.DataFrame, feature_set: str,
             all_leagues: List[str]) -> pd.DataFrame:
    league_dummies = pd.get_dummies(df["league"], dtype=float)
    for lg in all_leagues:
        if lg not in league_dummies.columns:
            league_dummies[lg] = 0.0
    league_dummies = league_dummies[all_leagues]
    if len(all_leagues) > 1:
        league_dummies = league_dummies.iloc[:, 1:]

    cluster_dummies = pd.get_dummies(
        df["cluster"].astype(str), prefix="cluster", dtype=float
    )
    all_cluster_cols = sorted(cluster_dummies.columns.tolist())
    if len(all_cluster_cols) > 1:
        cluster_dummies = cluster_dummies[all_cluster_cols[1:]]

    elo_rich = df[ELO_RICH_COLS].copy().reset_index(drop=True)
    ld = league_dummies.reset_index(drop=True)
    cd = cluster_dummies.reset_index(drop=True)

    if feature_set == "elo_rich":
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


# ── count model fitting ────────────────────────────────────────────────────

@dataclass
class FittedCountModel:
    family: str          # "poisson" or "negbin"
    params: np.ndarray
    alpha: float         # NegBin dispersion; 0 for Poisson
    aic_poisson: float
    aic_negbin: float
    columns: List[str]

    def predict_lambda(self, X: np.ndarray) -> np.ndarray:
        X_c = sm.add_constant(X.astype(float))
        return np.exp(X_c @ self.params[:X_c.shape[1]])

    def predict_over_prob(self, X: np.ndarray, line: float) -> np.ndarray:
        """P(total > line) for each row."""
        lam = self.predict_lambda(X)
        k = int(line)  # for line=1.5 → k=1, we want P(Y >= 2) = 1 - P(Y <= 1)
        if self.family == "poisson":
            return 1.0 - poisson.cdf(k, lam)
        else:
            # statsmodels NegBin parameterises as NB2: Var = mu + alpha*mu^2
            # scipy nbinom uses (n, p): n = 1/alpha, p = n/(n+mu)
            n = 1.0 / np.maximum(self.alpha, 1e-10)
            p = n / (n + lam)
            return 1.0 - nbinom.cdf(k, n, p)

    def predict_under_prob(self, X: np.ndarray, line: float) -> np.ndarray:
        return 1.0 - self.predict_over_prob(X, line)


def fit_count_model(y: np.ndarray, X: pd.DataFrame) -> FittedCountModel:
    X_c = sm.add_constant(X.astype(float))
    columns = list(X.columns)

    pois = sm.GLM(y, X_c.values, family=sm.families.Poisson()).fit()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        nb = NegativeBinomial(y, X_c.values).fit(disp=0, maxiter=300)

    nb_alpha = float(nb.params[-1]) if len(nb.params) > len(pois.params) else 0.0
    use_nb = nb.aic < pois.aic and nb_alpha > 1e-6

    if use_nb:
        return FittedCountModel(
            family="negbin", params=nb.params, alpha=nb_alpha,
            aic_poisson=float(pois.aic), aic_negbin=float(nb.aic),
            columns=columns,
        )
    return FittedCountModel(
        family="poisson", params=pois.params, alpha=0.0,
        aic_poisson=float(pois.aic), aic_negbin=float(nb.aic),
        columns=columns,
    )


# ── walk-forward engine ────────────────────────────────────────────────────

@dataclass
class FoldMetrics:
    fold: int
    test_start: str
    test_end: str
    n_train: int
    n_test: int
    family: str
    mean_lambda: float
    mae: float
    line_metrics: Dict[float, Dict[str, float]]  # line → {ll, brier, acc, base}


@dataclass
class BacktestResult:
    feature_set: str
    folds: List[FoldMetrics] = field(default_factory=list)
    all_y_total: List[int] = field(default_factory=list)
    all_lambda: List[float] = field(default_factory=list)
    all_over_probs: Dict[float, List[float]] = field(default_factory=dict)


def _log_loss(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    eps = 1e-15
    p = np.clip(y_prob, eps, 1 - eps)
    return -np.mean(y_true * np.log(p) + (1 - y_true) * np.log(1 - p))


def _brier(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    return float(np.mean((y_prob - y_true) ** 2))


def walk_forward(df: pd.DataFrame, feature_set: str) -> BacktestResult:
    result = BacktestResult(feature_set=feature_set)
    for line in LINES:
        result.all_over_probs[line] = []
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
        y_train = train["total_goals"].values
        y_test = test["total_goals"].values

        model = fit_count_model(y_train, X_train)

        pred_lambda = model.predict_lambda(X_test.values)
        result.all_y_total.extend(y_test.tolist())
        result.all_lambda.extend(pred_lambda.tolist())

        mae = float(np.mean(np.abs(y_test - pred_lambda)))
        line_metrics = {}
        for line in LINES:
            over_prob = model.predict_over_prob(X_test.values, line)
            result.all_over_probs[line].extend(over_prob.tolist())
            y_over = (y_test > line).astype(int)
            y_pred = (over_prob >= 0.5).astype(int)
            line_metrics[line] = {
                "log_loss": _log_loss(y_over, over_prob),
                "brier": _brier(y_over, over_prob),
                "accuracy": float(np.mean(y_pred == y_over)),
                "base_rate": float(y_over.mean()),
                "precision": float(y_over[y_pred == 1].mean()) if y_pred.sum() > 0 else 0.0,
                "recall": float(y_pred[y_over == 1].mean()) if y_over.sum() > 0 else 0.0,
            }

        fm = FoldMetrics(
            fold=fold_idx,
            test_start=str(test_start.date()),
            test_end=str(test_end.date()),
            n_train=len(train), n_test=len(test),
            family=model.family,
            mean_lambda=float(pred_lambda.mean()),
            mae=mae,
            line_metrics=line_metrics,
        )
        result.folds.append(fm)
        fold_idx += 1
        i += FOLD_MONTHS

    return result


# ── aggregation ─────────────────────────────────────────────────────────────

def aggregate(res: BacktestResult) -> Dict[str, Dict]:
    y = np.array(res.all_y_total)
    lam = np.array(res.all_lambda)
    out = {
        "n": len(y),
        "mae": float(np.mean(np.abs(y - lam))),
        "mean_actual": float(y.mean()),
        "mean_predicted": float(lam.mean()),
    }
    for line in LINES:
        probs = np.array(res.all_over_probs[line])
        y_over = (y > line).astype(int)
        y_pred = (probs >= 0.5).astype(int)
        out[f"over_{line}"] = {
            "log_loss": _log_loss(y_over, probs),
            "brier": _brier(y_over, probs),
            "accuracy": float(np.mean(y_pred == y_over)),
            "base_rate": float(y_over.mean()),
            "precision": float(y_over[y_pred == 1].mean()) if y_pred.sum() > 0 else 0.0,
            "recall": float(y_pred[y_over == 1].mean()) if y_over.sum() > 0 else 0.0,
        }
    return out


def calibration_table(res: BacktestResult, line: float) -> pd.DataFrame:
    y = np.array(res.all_y_total)
    probs = np.array(res.all_over_probs[line])
    y_over = (y > line).astype(int)
    rows = []
    for lo, hi in CALIBRATION_BINS:
        mask = (probs >= lo) & (probs < hi)
        n = mask.sum()
        if n == 0:
            continue
        rows.append({
            "prob_band": f"{lo:.1f}-{hi:.1f}",
            "n": int(n),
            "mean_pred": float(probs[mask].mean()),
            "observed_rate": float(y_over[mask].mean()),
            "gap": float(y_over[mask].mean() - probs[mask].mean()),
        })
    return pd.DataFrame(rows)


# ── reporting ───────────────────────────────────────────────────────────────

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

    print(f"\n{'='*72}")
    print(f"  GOALS COUNT-MODEL BACKTEST  ({len(df)} matches)")
    print(f"  {df['kickoff_utc'].min().date()} → {df['kickoff_utc'].max().date()}")
    print(f"  {FOLD_MONTHS}-month folds, Poisson vs NegBin (auto-selected)")
    print(f"  Lines: {LINES}")
    print(f"{'='*72}")

    results = {}
    for fs_name in FEATURE_SETS:
        _print_section(f"Feature set: {fs_name}")
        res = walk_forward(df, fs_name)
        results[fs_name] = res
        agg = aggregate(res)

        families = [f.family for f in res.folds]
        nb_pct = sum(1 for f in families if f == "negbin") / len(families) * 100

        print(f"  n={agg['n']}  MAE={agg['mae']:.3f}  "
              f"mean_pred={agg['mean_predicted']:.2f}  "
              f"mean_actual={agg['mean_actual']:.2f}  "
              f"NegBin={nb_pct:.0f}% of folds")
        for line in LINES:
            m = agg[f"over_{line}"]
            print(f"    over {line}: ll={m['log_loss']:.4f}  "
                  f"brier={m['brier']:.4f}  acc={m['accuracy']:.3f}  "
                  f"prec={m['precision']:.3f}  rec={m['recall']:.3f}  "
                  f"base={m['base_rate']:.3f}")

    # comparison table
    _print_section("Model comparison — all lines")
    comp_rows = []
    for fs_name in FEATURE_SETS:
        agg = aggregate(results[fs_name])
        for line in LINES:
            m = agg[f"over_{line}"]
            comp_rows.append({
                "feature_set": fs_name,
                "line": line,
                "log_loss": m["log_loss"],
                "brier": m["brier"],
                "accuracy": m["accuracy"],
                "precision": m["precision"],
                "recall": m["recall"],
                "base_rate": m["base_rate"],
                "mae": agg["mae"],
            })
    comp_df = pd.DataFrame(comp_rows)
    with pd.option_context("display.max_rows", None, "display.width", 180,
                           "display.float_format", "{:.4f}".format):
        print(comp_df.to_string(index=False))

    # calibration for best model at each line
    best_name = min(results, key=lambda k: aggregate(results[k])["over_2.5"]["log_loss"])
    for line in LINES:
        _print_section(f"Calibration — over {line} ({best_name})")
        cal = calibration_table(results[best_name], line)
        with pd.option_context("display.float_format", "{:.3f}".format):
            print(cal.to_string(index=False))

    # fold detail for best
    _print_section(f"Fold detail ({best_name})")
    fold_rows = []
    for f in results[best_name].folds:
        row = {
            "fold": f.fold,
            "test_window": f"{f.test_start} → {f.test_end}",
            "n_test": f.n_test,
            "family": f.family,
            "mean_λ": f.mean_lambda,
            "MAE": f.mae,
        }
        for line in LINES:
            row[f"ll_{line}"] = f.line_metrics[line]["log_loss"]
        fold_rows.append(row)
    fold_df = pd.DataFrame(fold_rows)
    with pd.option_context("display.max_rows", None, "display.width", 200,
                           "display.float_format", "{:.3f}".format):
        print(fold_df.to_string(index=False))

    if csv_dir:
        out = Path(csv_dir)
        out.mkdir(parents=True, exist_ok=True)
        comp_df.to_csv(out / "goals_comparison.csv", index=False)
        for line in LINES:
            cal = calibration_table(results[best_name], line)
            cal.to_csv(out / f"goals_calibration_{line}.csv", index=False)
        print(f"\nCSVs written to {out}")

    if close_conn:
        conn.close()

    return results


# ── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Walk-forward backtest: Poisson/NegBin count model for goals."
    )
    parser.add_argument("--csv", metavar="DIR",
                        help="Write result CSVs to this directory.")
    args = parser.parse_args()
    run(csv_dir=args.csv)
