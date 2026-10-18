"""League blend for Goals outlook (2026-10-17 proposal, owner approved)."""
import math

import pytest

from engine.eval.consensus_slips import attach_league_comparisons
from engine.eval.goals_league_blend import BLENDED_KEYS, blend_probability, load_blend

W = {"dixon_coles_team_over_1_5": {"a": 0.16, "b": 0.58, "c": 0.23}}
FIX = {"home": "Cuiaba", "away": "Ponte Preta", "league": "Serie B", "country": "Brazil"}
LG = "Serie B (Brazil)"


def _expected(p, rate, w):
    lg = lambda x: math.log(x / (1 - x))
    return 1 / (1 + math.exp(-(w["a"] + w["b"] * lg(p) + w["c"] * lg(rate))))


def test_blend_pulls_a_high_gap_chance_toward_the_league():
    out = blend_probability("Cuiaba Over 1.5", 0.69, 0.33, W)
    assert out == pytest.approx(_expected(0.69, 0.33, W["dixon_coles_team_over_1_5"]))
    assert out < 0.69


@pytest.mark.parametrize("market", ["Match Over 1.5", "Match Goals 1-4", "Cuiaba Win"])
def test_markets_outside_the_blend_are_untouched(market):
    assert blend_probability(market, 0.80, 0.50, {k: W["dixon_coles_team_over_1_5"]
                                                  for k in BLENDED_KEYS}) is None


def test_no_league_rate_or_low_chance_means_no_blend():
    assert blend_probability("Cuiaba Over 1.5", 0.69, None, W) is None
    assert blend_probability("Cuiaba Over 1.5", 0.55, 0.33, W) is None


def test_missing_weights_mean_no_blend():
    assert blend_probability("Cuiaba Over 1.5", 0.69, 0.33, {}) is None


def test_shipped_weights_cover_the_four_markets():
    assert set(load_blend()) == set(BLENDED_KEYS)


def test_attach_blends_before_gap_and_keeps_the_original(monkeypatch):
    import engine.eval.goals_league_blend as glb
    monkeypatch.setattr(glb, "_CACHE", W)
    rates = {(LG, "score_2goals", "away"): {"rate": 0.33, "rows": 100},
             (LG, "Over_1_5", "all"): {"rate": 0.70, "rows": 200},
             (LG, "score_2goals", "home"): {"rate": 0.40, "rows": 10}}
    fx = dict(FIX, bayesian=[], dixon_coles=[
        {"market": "Ponte Preta Over 1.5", "probability": 0.69, "tier": "x"},
        {"market": "Match Over 1.5", "probability": 0.85},
        {"market": "Cuiaba Over 1.5", "probability": 0.69},   # league rate on 10 fixtures
    ])
    attach_league_comparisons([fx], rates)
    team2, over15, thin = fx["dixon_coles"]
    want = round(_expected(0.69, 0.33, W["dixon_coles_team_over_1_5"]), 4)
    assert team2["probability"] == want and team2["probability_pre_blend"] == 0.69
    assert team2["league_blend"] is True
    assert team2["league_gap"] == pytest.approx(want - 0.33, abs=1e-4)
    assert "probability_pre_blend" not in over15 and over15["probability"] == 0.85
    assert "probability_pre_blend" not in thin and thin["league_comparison"] == "too_few_matches"
