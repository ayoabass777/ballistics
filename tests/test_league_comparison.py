"""League comparison (D57): DC estimates and pattern records vs league rates."""
import pytest

from engine.eval.consensus_slips import (
    attach_league_comparisons,
    dc_league_rate,
    league_comparison_label,
)

FIX = {"home": "Cuiaba", "away": "Ponte Preta", "league": "Serie B", "country": "Brazil"}
LG = "Serie B (Brazil)"
RATES = {
    (LG, "Over_1_5", "all"): {"rate": 0.70, "rows": 200},
    (LG, "goals_1_4", "fixtures"): {"rate": 0.82, "rows": 100},
    (LG, "score_1goal", "home"): {"rate": 0.72, "rows": 100},
    (LG, "score_1goal", "away"): {"rate": 0.60, "rows": 100},
    (LG, "BTTS", "all"): {"rate": 0.45, "rows": 30},  # only 15 fixtures
}


@pytest.mark.parametrize("gap,label", [
    (None, "unavailable"), (0.20, "well_above"), (0.15, "well_above"),
    (0.10, "above"), (0.05, "above"), (0.0, "typical"), (-0.049, "typical"),
    (-0.05, "below"), (-0.149, "below"), (-0.15, "well_below"),
])
def test_label_bands(gap, label):
    assert league_comparison_label(gap) == label


def test_match_event_uses_all_context_and_counts_fixtures_once():
    ref = dc_league_rate("Match Over 1.5", FIX, RATES)
    assert ref == {"event": "Over_1_5", "context": "all", "rate": 0.70, "matches": 100}


def test_goals_1_4_uses_fixture_level_rate():
    # "goalless" is a team event (failed to score), so 1-4 can't be derived
    # from Under_4_5; it comes from final scores, one row per fixture.
    ref = dc_league_rate("Match Goals 1-4", FIX, RATES)
    assert ref == {"event": "goals_1_4", "context": "all", "rate": 0.82, "matches": 100}


def test_team_event_uses_the_teams_venue():
    assert dc_league_rate("Cuiaba Over 0.5", FIX, RATES)["context"] == "home"
    away = dc_league_rate("Ponte Preta Over 0.5", FIX, RATES)
    assert (away["context"], away["rate"], away["matches"]) == ("away", 0.60, 100)


def test_unknown_market_or_missing_rate_is_none():
    assert dc_league_rate("Match Over 3.5", FIX, RATES) is None
    assert dc_league_rate("Someone Else Over 0.5", FIX, RATES) is None
    assert dc_league_rate("Match Corners", FIX, RATES) is None


def test_attach_labels_dc_and_pattern_rows():
    fx = dict(FIX, dixon_coles=[
        {"market": "Match Over 1.5", "probability": 0.85},
        {"market": "Match Goals 1-4", "probability": 0.83},
        {"market": "BTTS Yes", "probability": 0.70},
        {"market": "Match Over 3.5", "probability": 0.66},
    ], bayesian=[
        {"event": "loss", "k": 14, "n": 15, "base_rate": 0.45},
        {"event": "win", "k": 4, "n": 5, "base_rate": 0.40},
        {"event": "draw", "k": 2, "n": 0, "base_rate": 0.3},
    ])
    attach_league_comparisons([fx], RATES)
    over, bounds, btts, o35 = fx["dixon_coles"]
    assert (over["league_comparison"], over["league_gap"]) == ("well_above", 0.15)
    assert bounds["league_comparison"] == "typical"
    assert btts["league_comparison"] == "too_few_matches"
    assert btts["league_rate"] == 0.45  # still shown in details
    assert o35["league_comparison"] == "unavailable" and o35["league_rate"] is None
    loss, win, draw = fx["bayesian"]
    assert loss["league_comparison"] == "well_above"
    assert loss["league_gap"] == pytest.approx(14 / 15 - 0.45, abs=1e-4)
    assert win["league_comparison"] == "too_few_matches"
    assert draw["league_comparison"] == "unavailable"
