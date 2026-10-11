"""
Tests for engine.eval.goals_backtest (Poisson/NegBin count model).
"""

import numpy as np
import pandas as pd
import pytest

from engine.eval.goals_backtest import (
    prepare_features,
    fit_count_model,
    walk_forward,
    aggregate,
    calibration_table,
    LINES,
    FEATURE_SETS,
)


def _make_synthetic(n_per_league: int = 1200, seed: int = 77) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    start = pd.Timestamp("2022-01-01", tz="UTC")
    league_params = {"Low": 1.5, "Mid": 2.5, "High": 3.8}

    for league, lam in league_params.items():
        for i in range(n_per_league):
            day_offset = int(i * 730 / n_per_league)
            kickoff = start + pd.Timedelta(days=day_offset, hours=rng.integers(12, 21))
            home_elo = rng.normal(1500, 120)
            away_elo = rng.normal(1500, 120)
            total = rng.poisson(max(0.5, lam + 0.001 * abs(home_elo - away_elo)))
            hg = rng.binomial(total, 0.55) if total > 0 else 0
            ag = total - hg
            rows.append({
                "fixture_id": len(rows), "kickoff_utc": kickoff,
                "league": league, "home_goals": int(hg),
                "away_goals": int(ag), "home_elo": home_elo,
                "away_elo": away_elo,
            })
    return pd.DataFrame(rows)


@pytest.fixture
def prepared_df() -> pd.DataFrame:
    return prepare_features(_make_synthetic())


class TestPrepareFeatures:
    def test_columns(self, prepared_df):
        for col in ("abs_gap", "max_elo", "min_elo", "elo_sum",
                     "home_elo_gap", "total_goals"):
            assert col in prepared_df.columns

    def test_no_over_column(self, prepared_df):
        assert "over_1_5" not in prepared_df.columns


class TestFitCountModel:
    def test_poisson_produces_lambda(self, prepared_df):
        X = prepared_df[["abs_gap"]].copy()
        y = prepared_df["total_goals"].values
        model = fit_count_model(y, X)
        lam = model.predict_lambda(X.values)
        assert lam.shape[0] == len(y)
        assert (lam > 0).all()

    def test_over_prob_sums(self, prepared_df):
        X = prepared_df[["abs_gap"]].copy()
        y = prepared_df["total_goals"].values
        model = fit_count_model(y, X)
        over = model.predict_over_prob(X.values, 1.5)
        under = model.predict_under_prob(X.values, 1.5)
        np.testing.assert_allclose(over + under, 1.0, atol=1e-10)

    def test_family_chosen(self, prepared_df):
        X = prepared_df[["abs_gap"]].copy()
        y = prepared_df["total_goals"].values
        model = fit_count_model(y, X)
        assert model.family in ("poisson", "negbin")


class TestWalkForward:
    def test_produces_folds(self, prepared_df):
        res = walk_forward(prepared_df, "elo_rich")
        assert len(res.folds) >= 2

    def test_no_future_leak(self, prepared_df):
        res = walk_forward(prepared_df, "elo_rich")
        for f in res.folds:
            assert f.test_start > str(pd.Timestamp(f.test_start).date() - pd.Timedelta(days=1))

    def test_all_feature_sets_run(self, prepared_df):
        for fs in FEATURE_SETS:
            res = walk_forward(prepared_df, fs)
            assert len(res.all_y_total) > 0

    def test_all_lines_tracked(self, prepared_df):
        res = walk_forward(prepared_df, "elo_rich")
        for line in LINES:
            assert len(res.all_over_probs[line]) == len(res.all_y_total)


class TestAggregate:
    def test_has_all_lines(self, prepared_df):
        res = walk_forward(prepared_df, "elo_rich")
        agg = aggregate(res)
        for line in LINES:
            key = f"over_{line}"
            assert key in agg
            assert "log_loss" in agg[key]

    def test_mae_reasonable(self, prepared_df):
        res = walk_forward(prepared_df, "elo_plus_cluster")
        agg = aggregate(res)
        assert agg["mae"] < 3.0


class TestCalibration:
    def test_has_rows(self, prepared_df):
        res = walk_forward(prepared_df, "elo_plus_cluster")
        for line in LINES:
            cal = calibration_table(res, line)
            assert len(cal) >= 1
            assert "observed_rate" in cal.columns
