"""
Grid search over strategy parameters to find optimal threshold + feature combos.

Runs walk-forward backtest with different configurations and reports hit rates.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from itertools import product
from typing import List, Tuple

import numpy as np
import pandas as pd

from engine.eval.cluster_backtest import (
    ELO_COLS,
    FORM_COLS,
    STRENGTH_COLS,
    PERCENTILE_COLS,
    REST_COLS,
    VOL_COLS,
    INTERACT_COLS,
    ADAPTIVE_COLS,
    UNDER_COLS,
    OVER_COLS,
    LINES,
    MIN_TRAIN_PER_CLUSTER,
    MIN_TRAIN_TOTAL,
    assign_clusters_seasonal,
    assign_clusters_hierarchical,
    compute_form_features,
    compute_rest_days,
    compute_strength_features,
    prepare_features,
    _fit_poisson_lambda,
    _predict_poisson_over,
    _walk_forward_splits,
    load_backtest_df,
)


@dataclass
class TuneConfig:
    name: str
    over_cols: List[str]
    under_cols: List[str]
    over_thresh: float
    under_thresh: float
    line: float
    use_seasonal_cluster: bool = True


@dataclass
class TuneResult:
    config_name: str
    line: float
    over_thresh: float
    under_thresh: float
    over_picks: int = 0
    over_hits: int = 0
    under_picks: int = 0
    under_hits: int = 0
    total_games: int = 0
    n_folds: int = 0

    @property
    def over_hit_rate(self) -> float:
        return self.over_hits / self.over_picks if self.over_picks else 0.0

    @property
    def under_hit_rate(self) -> float:
        return self.under_hits / self.under_picks if self.under_picks else 0.0

    @property
    def total_picks(self) -> int:
        return self.over_picks + self.under_picks

    @property
    def total_hits(self) -> int:
        return self.over_hits + self.under_hits

    @property
    def combined_hit_rate(self) -> float:
        return self.total_hits / self.total_picks if self.total_picks else 0.0

    @property
    def picks_per_year(self) -> float:
        return self.total_picks / max(self.n_folds * 0.25, 0.25)


# ── Feature set combinations ──

FEATURE_SETS_OVER = {
    "elo_only": ELO_COLS,
    "elo+strength": ELO_COLS + STRENGTH_COLS,
    "elo+strength+rest": ELO_COLS + STRENGTH_COLS + REST_COLS,  # current OVER_COLS
    "elo+strength+pct": ELO_COLS + STRENGTH_COLS + PERCENTILE_COLS,
    "elo+strength+rest+pct": ELO_COLS + STRENGTH_COLS + REST_COLS + PERCENTILE_COLS,
    "elo+form": ELO_COLS + FORM_COLS,
    "elo+form+strength": ELO_COLS + FORM_COLS + STRENGTH_COLS,
    "full": ELO_COLS + STRENGTH_COLS + PERCENTILE_COLS + REST_COLS + FORM_COLS,
    # Dual-Elo: flat Elo (wide spread) + adaptive gap (league-aware)
    "dual_elo+str": ELO_COLS + ADAPTIVE_COLS + STRENGTH_COLS,
    "dual_elo+str+rest": ELO_COLS + ADAPTIVE_COLS + STRENGTH_COLS + REST_COLS,
    "dual_elo+full": ELO_COLS + ADAPTIVE_COLS + STRENGTH_COLS + PERCENTILE_COLS + REST_COLS,
}

FEATURE_SETS_UNDER = {
    "current_under": UNDER_COLS,
    "strength_only": STRENGTH_COLS,
    "strength+pct": STRENGTH_COLS + PERCENTILE_COLS,
    "strength+pct+rest": STRENGTH_COLS + PERCENTILE_COLS + REST_COLS,
    "strength+pct+elo": STRENGTH_COLS + PERCENTILE_COLS + ["abs_gap", "home_elo_gap"],
    "elo+strength+rest": ELO_COLS + STRENGTH_COLS + REST_COLS,
    # Dual-Elo under variants
    "dual+str+pct": STRENGTH_COLS + PERCENTILE_COLS + ADAPTIVE_COLS + ["abs_gap", "home_elo_gap"],
    "dual+under": UNDER_COLS + ADAPTIVE_COLS,
    "full": ELO_COLS + STRENGTH_COLS + PERCENTILE_COLS + REST_COLS + FORM_COLS,
}

THRESHOLDS = [0.52, 0.55, 0.58, 0.60, 0.63, 0.65, 0.70]


def run_grid_search(df: pd.DataFrame, lines: List[float] = None,
                    verbose: bool = True) -> pd.DataFrame:
    if lines is None:
        lines = [2.5, 3.5]

    # Precompute form features once
    df = compute_form_features(prepare_features(df))

    results = []
    total_combos = (len(FEATURE_SETS_OVER) * len(FEATURE_SETS_UNDER) *
                    len(THRESHOLDS) * len(THRESHOLDS) * len(lines))
    if verbose:
        print(f"Grid search: {len(FEATURE_SETS_OVER)} over feature sets × "
              f"{len(FEATURE_SETS_UNDER)} under feature sets × "
              f"{len(THRESHOLDS)} over thresholds × {len(THRESHOLDS)} under thresholds × "
              f"{len(lines)} lines")
        print(f"Total combinations: {total_combos}")
        print(f"Running walk-forward folds...\n")

    # Pre-run folds with all features computed
    fold_data = []
    for fold_idx, train, test, test_start, test_end in _walk_forward_splits(df):
        train_s = compute_strength_features(train, train)
        test_s = compute_strength_features(train, test)
        train_s = compute_rest_days(train_s)
        test_s = compute_rest_days(test_s)
        train_s["cluster"] = assign_clusters_seasonal(train_s, train_s)
        test_s["cluster"] = assign_clusters_seasonal(train_s, test_s)
        fold_data.append((fold_idx, train_s, test_s, test_start, test_end))

    if verbose:
        print(f"Pre-computed {len(fold_data)} folds")

    # For each over/under feature set combo, fit models once per fold,
    # then sweep thresholds cheaply
    combo_idx = 0
    total_feature_combos = len(FEATURE_SETS_OVER) * len(FEATURE_SETS_UNDER) * len(lines)

    for line in lines:
        for over_name, over_cols in FEATURE_SETS_OVER.items():
            for under_name, under_cols in FEATURE_SETS_UNDER.items():
                combo_idx += 1
                if verbose and combo_idx % 10 == 0:
                    print(f"  [{combo_idx}/{total_feature_combos}] "
                          f"line={line} over={over_name} under={under_name}")

                # Collect all predictions across folds
                all_over_probs = []
                all_under_probs = []
                all_y_true = []
                n_folds = 0

                for fold_idx, train_s, test_s, test_start, test_end in fold_data:
                    # Check all required columns exist
                    missing_over = [c for c in over_cols if c not in test_s.columns]
                    missing_under = [c for c in under_cols if c not in test_s.columns]
                    if missing_over or missing_under:
                        continue

                    n_folds += 1

                    # ── OVER: per-cluster Poisson ──
                    clusters = sorted(train_s["cluster"].unique())
                    over_probs = np.full(len(test_s), np.nan)

                    for cl in clusters:
                        tr_cl = train_s[train_s["cluster"] == cl]
                        te_cl = test_s[test_s["cluster"] == cl]
                        if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                            continue
                        try:
                            with warnings.catch_warnings():
                                warnings.simplefilter("ignore")
                                params = _fit_poisson_lambda(
                                    tr_cl["total_goals"].values,
                                    tr_cl[over_cols].values)
                            idx = te_cl.index
                            probs = _predict_poisson_over(
                                params, te_cl[over_cols].values, line)
                            over_probs[test_s.index.get_indexer(idx)] = probs
                        except Exception:
                            continue

                    nan_mask = np.isnan(over_probs)
                    if nan_mask.any():
                        over_probs[nan_mask] = (train_s["total_goals"] > line).mean()

                    # ── UNDER: global Poisson ──
                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            params_u = _fit_poisson_lambda(
                                train_s["total_goals"].values,
                                train_s[under_cols].values)
                        under_probs = 1.0 - _predict_poisson_over(
                            params_u, test_s[under_cols].values, line)
                    except Exception:
                        under_probs = np.full(len(test_s), 1.0 - (train_s["total_goals"] > line).mean())

                    y_true = (test_s["total_goals"].values > line).astype(int)

                    all_over_probs.append(over_probs)
                    all_under_probs.append(under_probs)
                    all_y_true.append(y_true)

                if not all_over_probs:
                    continue

                over_all = np.concatenate(all_over_probs)
                under_all = np.concatenate(all_under_probs)
                y_all = np.concatenate(all_y_true)

                # Sweep thresholds (cheap — just filtering)
                for over_thresh in THRESHOLDS:
                    for under_thresh in THRESHOLDS:
                        over_mask = over_all >= over_thresh
                        under_mask = under_all >= under_thresh
                        # Don't double-count: under only where not already over
                        under_mask = under_mask & ~over_mask

                        over_picks = int(over_mask.sum())
                        under_picks = int(under_mask.sum())

                        if over_picks + under_picks == 0:
                            continue

                        over_hits = int((y_all[over_mask] == 1).sum()) if over_picks else 0
                        under_hits = int((y_all[under_mask] == 0).sum()) if under_picks else 0

                        results.append({
                            "line": line,
                            "over_features": over_name,
                            "under_features": under_name,
                            "over_thresh": over_thresh,
                            "under_thresh": under_thresh,
                            "over_picks": over_picks,
                            "over_hits": over_hits,
                            "over_hit_rate": over_hits / over_picks if over_picks else 0,
                            "under_picks": under_picks,
                            "under_hits": under_hits,
                            "under_hit_rate": under_hits / under_picks if under_picks else 0,
                            "total_picks": over_picks + under_picks,
                            "total_hits": over_hits + under_hits,
                            "combined_rate": (over_hits + under_hits) / (over_picks + under_picks),
                            "picks_per_year": (over_picks + under_picks) / max(n_folds * 0.25, 0.25),
                            "n_folds": n_folds,
                        })

    results_df = pd.DataFrame(results)
    return results_df


def print_top_configs(results_df: pd.DataFrame, top_n: int = 30) -> None:
    if results_df.empty:
        print("No results.")
        return

    # Filter: at least 100 total picks and reasonable picks/year
    viable = results_df[
        (results_df["total_picks"] >= 100) &
        (results_df["picks_per_year"] >= 50)
    ].copy()

    if viable.empty:
        print("No viable configs found (need >= 100 picks, >= 50/year)")
        viable = results_df[results_df["total_picks"] >= 50].copy()

    # Score: combined_rate weighted by volume
    viable["score"] = viable["combined_rate"] * np.log1p(viable["total_picks"])

    for line in sorted(viable["line"].unique()):
        line_df = viable[viable["line"] == line]

        print(f"\n{'='*120}")
        print(f"  TOP CONFIGS — Line {line}")
        print(f"{'='*120}")

        # Best by combined hit rate (min 200 picks)
        high_vol = line_df[line_df["total_picks"] >= 200]
        if not high_vol.empty:
            print(f"\n── Best hit rate (≥200 picks) ──\n")
            best = high_vol.nlargest(top_n, "combined_rate")
            print(f"  {'Over Feats':<22s} {'Under Feats':<22s} {'O-Thr':>5s} {'U-Thr':>5s} "
                  f"{'O-Picks':>7s} {'O-Hit%':>6s} {'U-Picks':>7s} {'U-Hit%':>6s} "
                  f"{'Total':>6s} {'Comb%':>6s} {'/Year':>6s}")
            print(f"  {'─'*22} {'─'*22} {'─'*5} {'─'*5} "
                  f"{'─'*7} {'─'*6} {'─'*7} {'─'*6} "
                  f"{'─'*6} {'─'*6} {'─'*6}")
            for _, r in best.iterrows():
                print(f"  {r['over_features']:<22s} {r['under_features']:<22s} "
                      f"{r['over_thresh']:>4.0%} {r['under_thresh']:>4.0%} "
                      f"{r['over_picks']:>7d} {r['over_hit_rate']:>5.1%} "
                      f"{r['under_picks']:>7d} {r['under_hit_rate']:>5.1%} "
                      f"{r['total_picks']:>6d} {r['combined_rate']:>5.1%} "
                      f"{r['picks_per_year']:>5.0f}")

        # Best score (rate × volume)
        print(f"\n── Best score (rate × log volume) ──\n")
        best_score = line_df.nlargest(top_n, "score")
        print(f"  {'Over Feats':<22s} {'Under Feats':<22s} {'O-Thr':>5s} {'U-Thr':>5s} "
              f"{'O-Picks':>7s} {'O-Hit%':>6s} {'U-Picks':>7s} {'U-Hit%':>6s} "
              f"{'Total':>6s} {'Comb%':>6s} {'/Year':>6s}")
        print(f"  {'─'*22} {'─'*22} {'─'*5} {'─'*5} "
              f"{'─'*7} {'─'*6} {'─'*7} {'─'*6} "
              f"{'─'*6} {'─'*6} {'─'*6}")
        for _, r in best_score.iterrows():
            print(f"  {r['over_features']:<22s} {r['under_features']:<22s} "
                  f"{r['over_thresh']:>4.0%} {r['under_thresh']:>4.0%} "
                  f"{r['over_picks']:>7d} {r['over_hit_rate']:>5.1%} "
                  f"{r['under_picks']:>7d} {r['under_hit_rate']:>5.1%} "
                  f"{r['total_picks']:>6d} {r['combined_rate']:>5.1%} "
                  f"{r['picks_per_year']:>5.0f}")

    # Compare to current config
    print(f"\n{'='*120}")
    print(f"  CURRENT CONFIG COMPARISON")
    print(f"{'='*120}")
    current = results_df[
        (results_df["over_features"] == "elo+strength+rest") &
        (results_df["under_features"] == "current_under") &
        (results_df["over_thresh"] == 0.60) &
        (results_df["under_thresh"] == 0.58)
    ]
    if not current.empty:
        for _, r in current.iterrows():
            print(f"\n  Line {r['line']}: {r['total_picks']} picks, "
                  f"{r['combined_rate']:.1%} hit rate, "
                  f"{r['picks_per_year']:.0f}/year "
                  f"(over: {r['over_picks']} @ {r['over_hit_rate']:.1%}, "
                  f"under: {r['under_picks']} @ {r['under_hit_rate']:.1%})")


if __name__ == "__main__":
    from engine.eval.db import get_conn
    conn = get_conn()
    df = load_backtest_df(conn)
    conn.close()

    results = run_grid_search(df, lines=[2.5, 3.5], verbose=True)
    print_top_configs(results, top_n=15)
