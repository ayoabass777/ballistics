"""
Tests for engine.eval.league_clustering.
"""

import numpy as np
import pandas as pd
import pytest

from engine.eval.league_clustering import (
    build_league_profiles,
    build_goal_distributions,
    cluster_ward,
    cluster_jsd,
    CLUSTER_FEATURES,
    MAX_GOALS_FOR_DIST,
)


def _make_synthetic(seed: int = 77) -> pd.DataFrame:
    """Build fixture-level data with deliberately different scoring styles.

    Low-scoring: League_Def (λ=1.5), League_Grind (λ=1.6)
    Mid-scoring: League_Mid (λ=2.5), League_Avg (λ=2.6)
    High-scoring: League_Goals (λ=3.8), League_Open (λ=4.0)
    """
    rng = np.random.default_rng(seed)
    rows = []
    start = pd.Timestamp("2024-01-01", tz="UTC")
    league_params = {
        "League_Def": 1.5,
        "League_Grind": 1.6,
        "League_Mid": 2.5,
        "League_Avg": 2.6,
        "League_Goals": 3.8,
        "League_Open": 4.0,
    }

    for league, lam in league_params.items():
        for i in range(200):
            day = int(i * 365 / 200)
            kickoff = start + pd.Timedelta(days=day, hours=rng.integers(12, 21))
            total = rng.poisson(lam)
            hg = rng.binomial(total, 0.55) if total > 0 else 0
            ag = total - hg
            rows.append({
                "league": league, "fixture_id": len(rows),
                "home_goals": int(hg), "away_goals": int(ag),
                "fulltime_result": (
                    "W" if hg > ag else ("L" if ag > hg else "D")),
                "kickoff_utc": kickoff,
            })

    return pd.DataFrame(rows)


@pytest.fixture
def match_df() -> pd.DataFrame:
    return _make_synthetic()


@pytest.fixture
def profiles(match_df) -> pd.DataFrame:
    return build_league_profiles(match_df)


class TestBuildLeagueProfiles:
    def test_has_all_features(self, profiles):
        for col in CLUSTER_FEATURES:
            assert col in profiles.columns

    def test_all_leagues_present(self, profiles):
        assert set(profiles["league"]) == {
            "League_Def", "League_Grind", "League_Mid",
            "League_Avg", "League_Goals", "League_Open",
        }

    def test_high_scoring_higher_mean(self, profiles):
        high = profiles[profiles["league"] == "League_Goals"]["mean_goals"].iloc[0]
        low = profiles[profiles["league"] == "League_Def"]["mean_goals"].iloc[0]
        assert high > low + 1.0


class TestBuildGoalDistributions:
    def test_distributions_sum_to_one(self, match_df):
        dists = build_goal_distributions(match_df)
        for league, d in dists.items():
            np.testing.assert_allclose(d.sum(), 1.0, atol=1e-8)

    def test_correct_length(self, match_df):
        dists = build_goal_distributions(match_df)
        for d in dists.values():
            assert len(d) == MAX_GOALS_FOR_DIST + 1


class TestClusterWard:
    def test_produces_clusters(self, profiles):
        ward_df, Z, k = cluster_ward(profiles)
        assert "cluster" in ward_df.columns
        assert k >= 2

    def test_high_scoring_cluster_together(self, profiles):
        ward_df, _, _ = cluster_ward(profiles)
        goals_cl = ward_df[ward_df["league"] == "League_Goals"]["cluster"].iloc[0]
        open_cl = ward_df[ward_df["league"] == "League_Open"]["cluster"].iloc[0]
        assert goals_cl == open_cl, "High-scoring leagues should cluster together"

    def test_low_scoring_separate_from_high(self, profiles):
        ward_df, _, _ = cluster_ward(profiles)
        def_cl = ward_df[ward_df["league"] == "League_Def"]["cluster"].iloc[0]
        goals_cl = ward_df[ward_df["league"] == "League_Goals"]["cluster"].iloc[0]
        assert def_cl != goals_cl, "Low and high scoring should be in different clusters"


class TestClusterJSD:
    def test_produces_clusters(self, profiles, match_df):
        dists = build_goal_distributions(match_df)
        jsd_df, Z, k = cluster_jsd(profiles, dists)
        assert "cluster_jsd" in jsd_df.columns
        assert k >= 2

    def test_high_scoring_cluster_together(self, profiles, match_df):
        dists = build_goal_distributions(match_df)
        jsd_df, _, _ = cluster_jsd(profiles, dists)
        goals_cl = jsd_df[jsd_df["league"] == "League_Goals"]["cluster_jsd"].iloc[0]
        open_cl = jsd_df[jsd_df["league"] == "League_Open"]["cluster_jsd"].iloc[0]
        assert goals_cl == open_cl, "High-scoring leagues should cluster together in JSD"

    def test_low_scoring_separate_from_high(self, profiles, match_df):
        dists = build_goal_distributions(match_df)
        jsd_df, _, _ = cluster_jsd(profiles, dists)
        def_cl = jsd_df[jsd_df["league"] == "League_Def"]["cluster_jsd"].iloc[0]
        goals_cl = jsd_df[jsd_df["league"] == "League_Goals"]["cluster_jsd"].iloc[0]
        assert def_cl != goals_cl
