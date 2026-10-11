"""
Tests for engine.eval.team_strength.
"""

import numpy as np
import pandas as pd
import pytest

from engine.eval.team_strength import (
    compute_team_strength,
    load_team_df_from_matches,
    _shrink_rate,
    MIN_GAMES_CONFIDENT,
)


def _make_synthetic(seed: int = 42) -> pd.DataFrame:
    """Build fixture-level data with planted team strengths.

    League A: 10 teams, one strong attacker (id=100, ~3.0 gpg),
              one strong defender (id=101, concedes ~0.5 gpg),
              rest are average (~1.3 gpg scored, ~1.3 conceded).
    League B: 8 teams, uniformly mediocre (~1.0 gpg).
    """
    rng = np.random.default_rng(seed)
    rows = []
    start = pd.Timestamp("2024-01-01", tz="UTC")

    # League A: planted strong/weak teams
    teams_a = list(range(100, 110))
    for i in range(300):
        ht, at = rng.choice(teams_a, size=2, replace=False)
        day = int(i * 365 / 300)
        kickoff = start + pd.Timedelta(days=day, hours=rng.integers(12, 21))

        # Strong attacker (100): scores ~3.0 regardless of home/away
        hg_lam = 3.0 if ht == 100 else 1.3
        ag_lam = 3.0 if at == 100 else 1.3

        # Strong defender (101): opponents only score ~0.5 against them
        if at == 101:
            hg_lam = min(hg_lam, 0.5)
        if ht == 101:
            ag_lam = min(ag_lam, 0.5)

        hg = rng.poisson(hg_lam)
        ag = rng.poisson(ag_lam)

        rows.append({
            "league": "League_A", "home_team_id": ht, "away_team_id": at,
            "home_goals": int(hg), "away_goals": int(ag),
            "kickoff_utc": kickoff,
        })

    # League B: all mediocre
    teams_b = list(range(200, 208))
    for i in range(200):
        ht, at = rng.choice(teams_b, size=2, replace=False)
        day = int(i * 365 / 200)
        kickoff = start + pd.Timedelta(days=day, hours=rng.integers(12, 21))
        hg = rng.poisson(1.0)
        ag = rng.poisson(1.0)
        rows.append({
            "league": "League_B", "home_team_id": ht, "away_team_id": at,
            "home_goals": int(hg), "away_goals": int(ag),
            "kickoff_utc": kickoff,
        })

    return pd.DataFrame(rows)


@pytest.fixture
def team_df() -> pd.DataFrame:
    return load_team_df_from_matches(_make_synthetic())


@pytest.fixture
def result(team_df) -> pd.DataFrame:
    return compute_team_strength(team_df)


class TestShrinkRate:
    def test_zero_games_returns_league_mean(self):
        assert _shrink_rate(0, 0, 1.5, 0.1) == 1.5

    def test_large_n_stays_near_observed(self):
        shrunk = _shrink_rate(300, 100, 1.5, 0.1)
        assert abs(shrunk - 3.0) < 0.3

    def test_small_n_pulls_toward_mean(self):
        shrunk = _shrink_rate(6, 2, 1.5, 0.1)
        assert shrunk < 3.0
        assert shrunk > 1.5


class TestComputeTeamStrength:
    def test_has_all_columns(self, result):
        expected = {"league", "team_id", "games_played", "attack_rate",
                    "defense_rate", "attack_shrunk", "defense_shrunk",
                    "attack_percentile", "defense_percentile", "low_confidence"}
        assert expected.issubset(set(result.columns))

    def test_strong_attacker_top_percentile(self, result):
        team_100 = result[result["team_id"] == 100].iloc[0]
        assert team_100["attack_percentile"] >= 80.0, (
            f"Planted strong attacker should be top percentile, got {team_100['attack_percentile']}")

    def test_strong_defender_top_percentile(self, result):
        team_101 = result[result["team_id"] == 101].iloc[0]
        assert team_101["defense_percentile"] >= 80.0, (
            f"Planted strong defender should be top percentile, got {team_101['defense_percentile']}")

    def test_within_league_only(self, result):
        league_a = result[result["league"] == "League_A"]
        league_b = result[result["league"] == "League_B"]
        assert set(league_a["team_id"]) & set(league_b["team_id"]) == set()
        assert league_a["attack_percentile"].max() <= 100.0
        assert league_b["attack_percentile"].max() <= 100.0

    def test_percentiles_bounded(self, result):
        assert (result["attack_percentile"] >= 0).all()
        assert (result["attack_percentile"] <= 100).all()
        assert (result["defense_percentile"] >= 0).all()
        assert (result["defense_percentile"] <= 100).all()

    def test_low_confidence_flag(self, result):
        low = result[result["low_confidence"]]
        if not low.empty:
            assert (low["games_played"] < MIN_GAMES_CONFIDENT).all()

    def test_shrinkage_reduces_extremes(self, result):
        lg_a = result[result["league"] == "League_A"]
        raw_range = lg_a["attack_rate"].max() - lg_a["attack_rate"].min()
        shrunk_range = lg_a["attack_shrunk"].max() - lg_a["attack_shrunk"].min()
        assert shrunk_range <= raw_range, "Shrinkage should compress the range"
