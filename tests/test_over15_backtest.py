"""
Tests for engine.eval.over15_backtest (v2 — cluster-aware).

Uses synthetic data spanning 24 months with two leagues to verify
walk-forward machinery, cluster assignment, calibration, and feature importance.
"""

import numpy as np
import pandas as pd
import pytest

from engine.eval.over15_backtest import (
    prepare_features,
    walk_forward,
    aggregate_metrics,
    calibration_table,
    feature_importance,
    FEATURE_SETS,
)


def _make_synthetic(n_per_league: int = 1200, seed: int = 99) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    start = pd.Timestamp("2022-01-01", tz="UTC")
    league_params = {"Low": 1.6, "High": 3.5}

    for league, lam in league_params.items():
        for i in range(n_per_league):
            day_offset = int(i * 730 / n_per_league)
            kickoff = start + pd.Timedelta(days=day_offset, hours=rng.integers(12, 21))
            home_elo = rng.normal(1500, 120)
            away_elo = rng.normal(1500, 120)
            total = rng.poisson(lam)
            hg = rng.binomial(total, 0.55) if total > 0 else 0
            ag = total - hg
            rows.append({
                "fixture_id": len(rows),
                "kickoff_utc": kickoff,
                "league": league,
                "home_goals": int(hg),
                "away_goals": int(ag),
                "home_elo": home_elo,
                "away_elo": away_elo,
            })
    return pd.DataFrame(rows)


@pytest.fixture
def prepared_df() -> pd.DataFrame:
    return prepare_features(_make_synthetic())


class TestPrepareFeatures:
    def test_columns(self, prepared_df):
        for col in ("abs_gap", "max_elo", "min_elo", "elo_sum",
                     "home_elo_gap", "total_goals", "over_1_5"):
            assert col in prepared_df.columns

    def test_sorted(self, prepared_df):
        assert (prepared_df["kickoff_utc"].diff().dropna() >= pd.Timedelta(0)).all()


class TestWalkForward:
    def test_produces_folds(self, prepared_df):
        res = walk_forward(prepared_df, "elo_only")
        assert len(res.folds) >= 2

    def test_no_future_leak(self, prepared_df):
        res = walk_forward(prepared_df, "elo_only")
        for f in res.folds:
            assert f.train_end < f.test_start

    def test_all_feature_sets_run(self, prepared_df):
        for fs in FEATURE_SETS:
            res = walk_forward(prepared_df, fs)
            assert len(res.all_y_true) > 0

    def test_cluster_features_run(self, prepared_df):
        res = walk_forward(prepared_df, "elo_plus_cluster")
        assert len(res.folds) >= 2
        assert any("cluster" in k for d in res.coef_history for k in d)

    def test_elo_rich_has_more_features(self, prepared_df):
        elo_only = walk_forward(prepared_df, "elo_only")
        elo_rich = walk_forward(prepared_df, "elo_rich")
        n_elo = len(elo_only.coef_history[0])
        n_rich = len(elo_rich.coef_history[0])
        assert n_rich > n_elo


class TestCalibration:
    def test_has_rows(self, prepared_df):
        res = walk_forward(prepared_df, "elo_plus_cluster")
        cal = calibration_table(res)
        assert len(cal) >= 1
        assert "observed_rate" in cal.columns


class TestFeatureImportance:
    def test_abs_gap_appears(self, prepared_df):
        res = walk_forward(prepared_df, "elo_plus_cluster")
        fi = feature_importance(res)
        assert "abs_gap" in fi["feature"].values

    def test_cluster_features_appear(self, prepared_df):
        res = walk_forward(prepared_df, "elo_plus_cluster")
        fi = feature_importance(res)
        cluster_feats = [f for f in fi["feature"] if f.startswith("cluster_")]
        assert len(cluster_feats) >= 1
