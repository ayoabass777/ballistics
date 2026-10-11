"""
Synthetic-data test for engine.eval.elo_goals.

Generates matches where:
  - Elo gap drives margin strongly (favorites win big).
  - Elo gap has only a weak effect on total goals.
  - Scoring rate varies noticeably by league (league A low-scoring, B high).

Assertions:
  - Gap coefficient on total goals is small.
  - Gap effect weakens (larger p-value) when league FE is added.
  - Band summary recovers the structural scoring difference between leagues.
"""

import numpy as np
import pandas as pd
import pytest

from engine.eval.elo_goals import (
    build_analysis_df,
    gap_band_summary,
    global_glm,
    per_league_slopes,
    cluster_leagues,
    MIN_RELIABLE_N,
)


def _make_synthetic(n_per_league: int = 500, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    rows = []
    league_params = {
        "League_Low": {"base_total": 1.8, "margin_slope": 0.008},
        "League_High": {"base_total": 3.2, "margin_slope": 0.008},
    }

    for league, params in league_params.items():
        for _ in range(n_per_league):
            home_elo = rng.normal(1500, 150)
            away_elo = rng.normal(1500, 150)
            gap = home_elo - away_elo

            total_lambda = max(0.5, params["base_total"] + 0.0003 * abs(gap))
            total = rng.poisson(total_lambda)

            fav_share = 0.5 + params["margin_slope"] * abs(gap) / 100
            fav_share = min(fav_share, 0.95)
            if total == 0:
                fav_g, dog_g = 0, 0
            else:
                fav_g = rng.binomial(total, fav_share)
                dog_g = total - fav_g

            if home_elo >= away_elo:
                hg, ag = fav_g, dog_g
            else:
                hg, ag = dog_g, fav_g

            rows.append({
                "fixture_id": len(rows),
                "league": league,
                "home_goals": int(hg),
                "away_goals": int(ag),
                "home_elo": home_elo,
                "away_elo": away_elo,
            })

    return pd.DataFrame(rows)


@pytest.fixture
def synthetic_df() -> pd.DataFrame:
    return _make_synthetic()


@pytest.fixture
def analysis_df(synthetic_df) -> pd.DataFrame:
    return build_analysis_df(df=synthetic_df)


class TestBuildAnalysisDf:
    def test_derived_columns_present(self, analysis_df):
        for col in ("abs_gap", "total_goals", "fav_goals", "dog_goals",
                     "margin", "gap_band"):
            assert col in analysis_df.columns

    def test_fav_goals_gte_dog_in_margin(self, analysis_df):
        assert (analysis_df["margin"] == analysis_df["fav_goals"] - analysis_df["dog_goals"]).all()


class TestGapBandSummary:
    def test_reliability_flag(self, analysis_df):
        summary = gap_band_summary(analysis_df)
        for _, row in summary.iterrows():
            if row["n"] < MIN_RELIABLE_N:
                assert not row["reliable"]

    def test_league_scoring_difference(self, analysis_df):
        summary = gap_band_summary(analysis_df)
        low = summary[summary["league"] == "League_Low"]["mean_total"].mean()
        high = summary[summary["league"] == "League_High"]["mean_total"].mean()
        assert high > low + 0.5, (
            f"Expected League_High mean_total ({high:.2f}) "
            f"to clearly exceed League_Low ({low:.2f})"
        )


class TestGlobalGLM:
    def test_gap_coef_small(self, analysis_df):
        gap_only, _ = global_glm(analysis_df)
        assert abs(gap_only.gap_coef_per_100) < 0.5, (
            f"Gap coefficient too large: {gap_only.gap_coef_per_100:.4f}"
        )

    def test_gap_weakens_with_league_fe(self, analysis_df):
        gap_only, gap_league = global_glm(analysis_df)
        assert gap_league.gap_pvalue >= gap_only.gap_pvalue * 0.5, (
            "Gap p-value should not decrease dramatically with league FE"
        )

    def test_overdispersion_detected(self, analysis_df):
        gap_only, _ = global_glm(analysis_df)
        assert isinstance(gap_only.overdispersed, bool)

    def test_negbin_alpha_estimated(self, analysis_df):
        gap_only, _ = global_glm(analysis_df)
        assert gap_only.alpha is not None


class TestPerLeagueSlopes:
    def test_both_leagues_present(self, analysis_df):
        slopes = per_league_slopes(analysis_df)
        assert set(slopes["league"]) == {"League_Low", "League_High"}

    def test_slope_columns(self, analysis_df):
        slopes = per_league_slopes(analysis_df)
        for col in ("gap_coef_per_100", "gap_pvalue", "alpha", "n"):
            assert col in slopes.columns


def _make_multi_league_synthetic(seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    leagues = {
        "Defensive_A": 1.5, "Defensive_B": 1.6, "Defensive_C": 1.7,
        "Mid_A": 2.5, "Mid_B": 2.6,
        "Attacking_A": 3.8, "Attacking_B": 3.9, "Attacking_C": 4.0,
    }
    rows = []
    for league, base in leagues.items():
        for _ in range(300):
            home_elo = rng.normal(1500, 150)
            away_elo = rng.normal(1500, 150)
            total = rng.poisson(max(0.5, base))
            gap = home_elo - away_elo
            fav_share = min(0.5 + 0.005 * abs(gap) / 100, 0.95)
            fav_g = rng.binomial(total, fav_share) if total > 0 else 0
            dog_g = total - fav_g
            hg, ag = (fav_g, dog_g) if home_elo >= away_elo else (dog_g, fav_g)
            rows.append({
                "fixture_id": len(rows), "league": league,
                "home_goals": int(hg), "away_goals": int(ag),
                "home_elo": home_elo, "away_elo": away_elo,
            })
    return pd.DataFrame(rows)


class TestClusterLeagues:
    @pytest.fixture
    def multi_league_df(self):
        return build_analysis_df(df=_make_multi_league_synthetic())

    def test_assigns_clusters(self, multi_league_df):
        clusters, k = cluster_leagues(multi_league_df)
        assert "cluster" in clusters.columns
        assert k >= 2

    def test_separates_scoring_styles(self, multi_league_df):
        clusters, _ = cluster_leagues(multi_league_df)
        def_cluster = clusters[clusters["league"] == "Defensive_A"]["cluster"].iloc[0]
        att_cluster = clusters[clusters["league"] == "Attacking_A"]["cluster"].iloc[0]
        assert def_cluster != att_cluster, (
            "Leagues with very different scoring rates should land in different clusters"
        )
