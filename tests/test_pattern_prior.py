"""D129: per-(league, event, context) pattern prior from team spread."""
from engine.eval.consensus_slips import (
    PATTERN_PRIOR_CAP, PATTERN_PRIOR_FLOOR, DISCOVER_PATTERN_PRIOR_MATCHES,
    discover_candidates, pattern_prior_matches)


def _fixtures(rates, n=20, event="win", league="L (X)"):
    rows = [{"team": f"T{i}", "league": league, "event": event, "context": "all",
             "k": round(r * n), "n": n, "base_rate": 0.4} for i, r in enumerate(rates)]
    return [{"bayesian": rows}]


def test_even_league_gets_the_cap():
    priors = pattern_prior_matches(_fixtures([0.40] * 12))
    assert priors[("L (X)", "win", "all")] == PATTERN_PRIOR_CAP


def test_lopsided_league_gets_the_floor():
    priors = pattern_prior_matches(_fixtures([0.05, 0.9] * 6))
    assert priors[("L (X)", "win", "all")] == PATTERN_PRIOR_FLOOR


def test_too_few_teams_has_no_entry():
    assert pattern_prior_matches(_fixtures([0.1, 0.9] * 4)) == {}


def test_candidates_use_the_prior_and_fall_back():
    row = {"team": "T0", "league": "L (X)", "event": "win", "context": "all",
           "k": 9, "n": 10, "base_rate": 0.4, "league_comparison": "well_above"}
    fx = {"bayesian": [row]}
    (c,) = [c for c in discover_candidates(fx, None, None, {}, pattern_priors={("L (X)", "win", "all"): 30})
            if c["list"] == "patterns"]
    assert c["prior_matches"] == 30
    assert abs(c["score"] - ((9 + 30 * 0.4) / 40 - 0.4)) < 1e-9
    (d,) = [c for c in discover_candidates(fx, None, None, {}) if c["list"] == "patterns"]
    assert d["prior_matches"] == DISCOVER_PATTERN_PRIOR_MATCHES
