"""
Tests for engine.eval.cluster_backtest.
"""

import numpy as np
import pandas as pd
import pytest

from engine.eval.cluster_backtest import (
    prepare_features,
    compute_form_features,
    compute_strength_features,
    run_hier_cluster_poisson,
    run_hier_cluster_poisson_strength,
    run_per_cluster_balanced_lr,
    run_per_cluster_poisson,
    run_per_cluster_poisson_form,
    run_per_cluster_poisson_strength,
    run_per_cluster_poisson_vol,
    run_per_cluster_split_poisson,
    run_seasonal_cluster_poisson,
    run_seasonal_cluster_poisson_strength,
    run_global_dixon_coles,
    run_seasonal_cluster_dixon_coles,
    run_global_lgbm_poisson,
    run_global_lgbm_classifier,
    run_global_lgbm_strength,
    run_global_lgbm_full,
    run_global_poisson_full,
    run_seasonal_cluster_lgbm,
    run_seasonal_cluster_poisson_full,
    compute_rest_days,
    run_over_under_strategy,
    strategy_report,
    PERCENTILE_COLS,
    REST_COLS,
    run_global_balanced_lr,
    run_global_negbin_interact,
    run_global_negbin_strength,
    run_global_poisson,
    run_global_poisson_form,
    run_global_poisson_interact,
    run_global_poisson_strength,
    run_global_poisson_vol,
    run_global_split_poisson,
    aggregate,
    calibration_table,
    per_cluster_detail,
    LINES,
    INTERACT_COLS,
    STRENGTH_COLS,
)


def _make_synthetic(seed: int = 55) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    start = pd.Timestamp("2022-01-01", tz="UTC")
    leagues = {"DefA": 1.5, "DefB": 1.6, "MidA": 2.5, "HiA": 3.5, "HiB": 3.8}
    n_teams_per_league = 10

    for league, lam in leagues.items():
        teams = list(range(n_teams_per_league))
        for i in range(600):
            day_offset = int(i * 730 / 600)
            kickoff = start + pd.Timedelta(days=day_offset, hours=rng.integers(12, 21))
            home_elo = rng.normal(1500, 150)
            away_elo = rng.normal(1500, 150)
            total = rng.poisson(max(0.5, lam + 0.001 * abs(home_elo - away_elo)))
            hg = rng.binomial(total, 0.55) if total > 0 else 0
            ht, at = rng.choice(teams, size=2, replace=False)
            rows.append({
                "fixture_id": len(rows), "kickoff_utc": kickoff,
                "season": kickoff.year,
                "league": league, "home_goals": int(hg),
                "away_goals": int(total - hg),
                "home_elo": home_elo, "away_elo": away_elo,
                "home_team_id": int(f"{list(leagues).index(league)}{ht:02d}"),
                "away_team_id": int(f"{list(leagues).index(league)}{at:02d}"),
            })
    return pd.DataFrame(rows)


@pytest.fixture
def prepared_df() -> pd.DataFrame:
    return compute_form_features(prepare_features(_make_synthetic()))


class TestRunners:
    def test_per_cluster_balanced_lr(self, prepared_df):
        res = run_per_cluster_balanced_lr(prepared_df)
        assert len(res.folds) >= 2
        for line in LINES:
            assert len(res.all_y_true[line]) > 0

    def test_per_cluster_poisson(self, prepared_df):
        res = run_per_cluster_poisson(prepared_df)
        assert len(res.folds) >= 2

    def test_global_balanced_lr(self, prepared_df):
        res = run_global_balanced_lr(prepared_df)
        assert len(res.folds) >= 2

    def test_global_poisson(self, prepared_df):
        res = run_global_poisson(prepared_df)
        assert len(res.folds) >= 2

    def test_global_poisson_form(self, prepared_df):
        res = run_global_poisson_form(prepared_df)
        assert len(res.folds) >= 2

    def test_per_cluster_poisson_form(self, prepared_df):
        res = run_per_cluster_poisson_form(prepared_df)
        assert len(res.folds) >= 2

    def test_global_split_poisson(self, prepared_df):
        res = run_global_split_poisson(prepared_df)
        assert len(res.folds) >= 2

    def test_per_cluster_split_poisson(self, prepared_df):
        res = run_per_cluster_split_poisson(prepared_df)
        assert len(res.folds) >= 2

    def test_global_poisson_vol(self, prepared_df):
        res = run_global_poisson_vol(prepared_df)
        assert len(res.folds) >= 2

    def test_per_cluster_poisson_vol(self, prepared_df):
        res = run_per_cluster_poisson_vol(prepared_df)
        assert len(res.folds) >= 2

    def test_global_poisson_strength(self, prepared_df):
        res = run_global_poisson_strength(prepared_df)
        assert len(res.folds) >= 2

    def test_per_cluster_poisson_strength(self, prepared_df):
        res = run_per_cluster_poisson_strength(prepared_df)
        assert len(res.folds) >= 2

    def test_global_poisson_interact(self, prepared_df):
        res = run_global_poisson_interact(prepared_df)
        assert len(res.folds) >= 2

    def test_global_negbin_strength(self, prepared_df):
        res = run_global_negbin_strength(prepared_df)
        assert len(res.folds) >= 2

    def test_global_negbin_interact(self, prepared_df):
        res = run_global_negbin_interact(prepared_df)
        assert len(res.folds) >= 2

    def test_hier_cluster_poisson(self, prepared_df):
        res = run_hier_cluster_poisson(prepared_df)
        assert len(res.folds) >= 2

    def test_hier_cluster_poisson_strength(self, prepared_df):
        res = run_hier_cluster_poisson_strength(prepared_df)
        assert len(res.folds) >= 2

    def test_seasonal_cluster_poisson(self, prepared_df):
        res = run_seasonal_cluster_poisson(prepared_df)
        assert len(res.folds) >= 2

    def test_seasonal_cluster_poisson_strength(self, prepared_df):
        res = run_seasonal_cluster_poisson_strength(prepared_df)
        assert len(res.folds) >= 2

    def test_global_dixon_coles(self, prepared_df):
        res = run_global_dixon_coles(prepared_df)
        assert len(res.folds) >= 2

    def test_seasonal_cluster_dixon_coles(self, prepared_df):
        res = run_seasonal_cluster_dixon_coles(prepared_df)
        assert len(res.folds) >= 2

    def test_global_lgbm_poisson(self, prepared_df):
        res = run_global_lgbm_poisson(prepared_df)
        assert len(res.folds) >= 2

    def test_global_lgbm_classifier(self, prepared_df):
        res = run_global_lgbm_classifier(prepared_df)
        assert len(res.folds) >= 2

    def test_global_lgbm_strength(self, prepared_df):
        res = run_global_lgbm_strength(prepared_df)
        assert len(res.folds) >= 2

    def test_seasonal_cluster_lgbm(self, prepared_df):
        res = run_seasonal_cluster_lgbm(prepared_df)
        assert len(res.folds) >= 2

    def test_global_poisson_full(self, prepared_df):
        res = run_global_poisson_full(prepared_df)
        assert len(res.folds) >= 2

    def test_global_lgbm_full(self, prepared_df):
        res = run_global_lgbm_full(prepared_df)
        assert len(res.folds) >= 2

    def test_seasonal_cluster_poisson_full(self, prepared_df):
        res = run_seasonal_cluster_poisson_full(prepared_df)
        assert len(res.folds) >= 2


class TestComputeRestDays:
    def test_adds_rest_columns(self, prepared_df):
        out = compute_rest_days(prepared_df)
        for col in REST_COLS:
            assert col in out.columns

    def test_no_nans(self, prepared_df):
        out = compute_rest_days(prepared_df)
        for col in REST_COLS:
            assert not out[col].isna().any()

    def test_positive_values(self, prepared_df):
        out = compute_rest_days(prepared_df)
        for col in REST_COLS:
            assert (out[col] >= 0).all()

    def test_capped_at_30(self, prepared_df):
        out = compute_rest_days(prepared_df)
        for col in REST_COLS:
            assert (out[col] <= 30.0).all()


class TestComputeStrengthFeatures:
    def test_adds_strength_columns(self, prepared_df):
        out = compute_strength_features(prepared_df, prepared_df)
        for col in STRENGTH_COLS + INTERACT_COLS + PERCENTILE_COLS:
            assert col in out.columns, f"Missing {col}"

    def test_no_nans(self, prepared_df):
        out = compute_strength_features(prepared_df, prepared_df)
        for col in STRENGTH_COLS + INTERACT_COLS + PERCENTILE_COLS:
            assert not out[col].isna().any(), f"NaN in {col}"

    def test_values_positive(self, prepared_df):
        out = compute_strength_features(prepared_df, prepared_df)
        for col in STRENGTH_COLS + INTERACT_COLS + PERCENTILE_COLS:
            assert (out[col] >= 0).all(), f"Negative values in {col}"


class TestBalancedLRPredictsBothClasses:
    def test_predicts_under(self, prepared_df):
        res = run_per_cluster_balanced_lr(prepared_df)
        probs = np.array(res.all_y_prob[1.5])
        assert (probs < 0.5).sum() > 0, (
            "Balanced LR should predict some games as under 1.5"
        )


class TestAggregate:
    def test_has_all_lines(self, prepared_df):
        res = run_per_cluster_balanced_lr(prepared_df)
        agg = aggregate(res)
        for line in LINES:
            assert f"over_{line}" in agg

    def test_pred_positive_rate(self, prepared_df):
        res = run_per_cluster_balanced_lr(prepared_df)
        agg = aggregate(res)
        rate = agg["over_1.5"]["pred_positive_rate"]
        assert 0.1 < rate < 0.99, (
            f"Balanced model should not predict all-positive: pred+={rate:.3f}"
        )


class TestPerClusterDetail:
    def test_has_all_clusters(self, prepared_df):
        res = run_per_cluster_balanced_lr(prepared_df)
        detail = per_cluster_detail(res)
        assert not detail.empty
        assert set(detail["cluster"].unique()) == set(res.all_clusters)

    def test_has_all_lines(self, prepared_df):
        res = run_per_cluster_balanced_lr(prepared_df)
        detail = per_cluster_detail(res)
        for line in LINES:
            assert line in detail["line"].values

    def test_global_returns_empty(self, prepared_df):
        res = run_global_balanced_lr(prepared_df)
        detail = per_cluster_detail(res)
        assert detail.empty


class TestCalibration:
    def test_has_rows(self, prepared_df):
        res = run_per_cluster_balanced_lr(prepared_df)
        cal = calibration_table(res, 2.5)
        assert len(cal) >= 1


class TestOverUnderStrategy:
    def test_runs(self, prepared_df):
        strat = run_over_under_strategy(prepared_df, over_thresh=0.55,
                                        under_thresh=0.55)
        assert len(strat.fixture_ids) > 0

    def test_directions_valid(self, prepared_df):
        strat = run_over_under_strategy(prepared_df, over_thresh=0.55,
                                        under_thresh=0.55)
        for d in strat.directions:
            assert d in ("over", "under")

    def test_report(self, prepared_df):
        strat = run_over_under_strategy(prepared_df, over_thresh=0.55,
                                        under_thresh=0.55)
        report = strategy_report(strat)
        assert not report.empty
        assert "hit" in report.columns
        assert set(report["direction"].unique()).issubset({"over", "under"})
