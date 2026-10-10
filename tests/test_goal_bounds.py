"""Focused regression tests for the directly calibrated DC 1-4 market."""

import unittest

import numpy as np
import pandas as pd

from engine.eval import predict_dixon_coles as dc
from engine.eval.calibration_wf import (
    DC_BOUNDS_1_4_CALIBRATION_KEY,
    DC_BOUNDS_1_4_MARKET,
    _actual_outcome,
    _calibration_key,
)
from engine.eval.consensus_slips import (
    _build_bounds_1_4_shortlist,
    _build_over_1_5_shortlist,
    _bounds_1_4_league_prior,
    _score_pool,
    _sportify_market,
)
from engine.eval.results_review import _grade


class GoalBoundsTests(unittest.TestCase):
    def setUp(self):
        self.params = {
            "mu": 0.15,
            "home_adv": 0.2,
            "rho": -0.05,
            "attack": {1: 0.1, 2: -0.1},
            "defence": {1: -0.05, 2: 0.05},
        }

    def test_dc_market_is_exact_score_matrix_region(self):
        matrix, _lam, _mu = dc.score_matrix(self.params, 1, 2)
        ii, jj = np.indices(matrix.shape)
        expected = float(matrix[((ii + jj) >= 1) & ((ii + jj) <= 4)].sum())

        priced = dc.fixture_markets(self.params, 1, 2, "Home", "Away")

        self.assertAlmostEqual(priced["markets"][dc.BOUNDS_1_4_MARKET], expected)

    def test_bounds_outcome_includes_one_and_four_only(self):
        def row(hg, ag):
            return pd.Series({"goals_home_ft": hg, "goals_away_ft": ag})

        self.assertFalse(_actual_outcome(row(0, 0), DC_BOUNDS_1_4_MARKET))
        self.assertTrue(_actual_outcome(row(1, 0), DC_BOUNDS_1_4_MARKET))
        self.assertTrue(_actual_outcome(row(2, 2), DC_BOUNDS_1_4_MARKET))
        self.assertFalse(_actual_outcome(row(3, 2), DC_BOUNDS_1_4_MARKET))

    def test_bounds_uses_dedicated_calibration_key(self):
        prediction = {"model": "dixon_coles", "market": DC_BOUNDS_1_4_MARKET}
        self.assertEqual(_calibration_key(prediction), DC_BOUNDS_1_4_CALIBRATION_KEY)
        self.assertEqual(
            _calibration_key({"model": "dixon_coles", "market": "Match Over 1.5"}),
            "dixon_coles_over_1_5",
        )

    def test_live_calibrator_prefers_bounds_map(self):
        old_general = dc._DC_POINTS
        old_bounds = dc._DC_BOUNDS_1_4_POINTS
        old_leagues = dc._DC_BOUNDS_1_4_LEAGUE_ADJUSTMENTS
        old_market = dc._DC_MARKET_POINTS
        try:
            dc._DC_MARKET_POINTS = {}
            dc._DC_POINTS = [[0.0, 0.1], [1.0, 0.1]]
            dc._DC_BOUNDS_1_4_POINTS = [[0.0, 0.9], [1.0, 0.9]]
            dc._DC_BOUNDS_1_4_LEAGUE_ADJUSTMENTS = {}
            self.assertAlmostEqual(dc._calibrate(0.75, dc.BOUNDS_1_4_MARKET), 0.9)
            self.assertAlmostEqual(dc._calibrate(0.75, "Match Over 1.5"), 0.1)
        finally:
            dc._DC_MARKET_POINTS = old_market
            dc._DC_POINTS = old_general
            dc._DC_BOUNDS_1_4_POINTS = old_bounds
            dc._DC_BOUNDS_1_4_LEAGUE_ADJUSTMENTS = old_leagues

    def test_bounds_league_adjustment_is_optional_and_monotonic(self):
        old_bounds = dc._DC_BOUNDS_1_4_POINTS
        old_leagues = dc._DC_BOUNDS_1_4_LEAGUE_ADJUSTMENTS
        try:
            dc._DC_BOUNDS_1_4_POINTS = [[0.0, 0.8], [1.0, 0.8]]
            dc._DC_BOUNDS_1_4_LEAGUE_ADJUSTMENTS = {
                "140": {"logit_delta": 0.2},
            }
            global_only = dc._calibrate(0.75, dc.BOUNDS_1_4_MARKET)
            league_aware = dc._calibrate(0.75, dc.BOUNDS_1_4_MARKET, 140)
            unknown_league = dc._calibrate(0.75, dc.BOUNDS_1_4_MARKET, 999)
            self.assertGreater(league_aware, global_only)
            self.assertAlmostEqual(unknown_league, global_only)
        finally:
            dc._DC_BOUNDS_1_4_POINTS = old_bounds
            dc._DC_BOUNDS_1_4_LEAGUE_ADJUSTMENTS = old_leagues

    def test_consensus_and_results_use_same_bookmaker_shape(self):
        market = _sportify_market(DC_BOUNDS_1_4_MARKET, "Home", "Away")
        self.assertEqual(
            market,
            {"market": "Goal Bounds", "selection": "1-4", "line": None},
        )
        self.assertFalse(_grade("Goal Bounds", "1-4", None, 0, 0, "Home", "Away"))
        self.assertTrue(_grade("Goal Bounds", "1-4", None, 2, 2, "Home", "Away"))
        self.assertFalse(_grade("Goal Bounds", "1-4", None, 4, 1, "Home", "Away"))

    def test_near_threshold_bounds_is_watch_only(self):
        key = (123, DC_BOUNDS_1_4_MARKET)
        consensus = {
            key: {
                "fixture_id": 123,
                "unified_market": DC_BOUNDS_1_4_MARKET,
                "models": ["dixon_coles"],
                "confidences": [0.775],
                "details": [{"calibration_scope": "global+league",
                             "global_calibrated": 0.77,
                             "league_adjustment": 0.03}],
            }
        }
        fixture_meta = {
            123: {"home": "Home", "away": "Away", "league": "Test League",
                  "country": "Testland", "kickoff": "2026-08-11T18:00:00Z"}
        }
        pool, gates, watch = _score_pool(
            consensus, fixture_meta, {}, {}, frozenset())
        self.assertEqual(pool, [])
        self.assertEqual(len(watch), 1)
        self.assertEqual(watch[0]["decision_status"], "watch")
        self.assertEqual(watch[0]["calibration_scope"], "global+league")
        self.assertFalse(gates[key]["in_pool"])

    def test_bounds_at_78_percent_clears_the_solo_gate(self):
        key = (124, DC_BOUNDS_1_4_MARKET)
        consensus = {
            key: {
                "fixture_id": 124,
                "unified_market": DC_BOUNDS_1_4_MARKET,
                "models": ["dixon_coles"],
                "confidences": [0.78],
                "details": [{}],
            }
        }
        fixture_meta = {
            124: {"home": "Home", "away": "Away", "league": "Test League",
                  "country": "Testland", "kickoff": "2026-08-11T18:00:00Z"}
        }

        pool, gates, watch = _score_pool(
            consensus, fixture_meta, {}, {}, frozenset())

        self.assertEqual(len(pool), 1)
        self.assertTrue(gates[key]["in_pool"])
        self.assertEqual(watch, [])

    def test_bounds_at_70_percent_is_retained_as_searchable_watch_candidate(self):
        key = (125, DC_BOUNDS_1_4_MARKET)
        consensus = {
            key: {
                "fixture_id": 125,
                "unified_market": DC_BOUNDS_1_4_MARKET,
                "models": ["dixon_coles"],
                "confidences": [0.70],
                "details": [{}],
            }
        }
        fixture_meta = {
            125: {"home": "Home", "away": "Away", "league": "Serie B",
                  "country": "Italy", "kickoff": "2026-08-11T18:00:00Z"}
        }

        pool, gates, watch = _score_pool(
            consensus, fixture_meta, {}, {}, frozenset())

        self.assertEqual(pool, [])
        self.assertFalse(gates[key]["in_pool"])
        self.assertEqual(len(watch), 1)
        self.assertEqual(watch[0]["country"], "Italy")

    def test_bounds_league_prior_only_applies_to_selected_market_and_leagues(self):
        prior = _bounds_1_4_league_prior(
            DC_BOUNDS_1_4_MARKET, "Serie A", "Brazil")
        self.assertIsNotNone(prior)
        self.assertGreater(prior["boost"], 0)
        self.assertEqual(prior["rate"], 0.8298)
        self.assertIsNone(
            _bounds_1_4_league_prior("Match Under 3.5", "Serie A", "Brazil"))
        self.assertIsNone(
            _bounds_1_4_league_prior(DC_BOUNDS_1_4_MARKET, "Serie A", "Italy"))

    def test_bounds_league_prior_reorders_qualified_consensus_without_changing_confidence(self):
        def pick(fid):
            return {
                "fixture_id": fid,
                "unified_market": DC_BOUNDS_1_4_MARKET,
                "models": ["dixon_coles"],
                "confidences": [0.82],
                "details": [{}],
            }

        consensus = {
            (1, DC_BOUNDS_1_4_MARKET): pick(1),
            (2, DC_BOUNDS_1_4_MARKET): pick(2),
        }
        fixture_meta = {
            1: {"home": "A", "away": "B", "league": "Test League",
                "country": "Testland", "kickoff": "2026-08-11T18:00:00Z"},
            2: {"home": "C", "away": "D", "league": "Serie A",
                "country": "Brazil", "kickoff": "2026-08-11T18:00:00Z"},
        }

        pool, _gates, _watch = _score_pool(
            consensus, fixture_meta, {}, {}, frozenset())

        self.assertEqual([p["fixture_id"] for p in pool], [2, 1])
        self.assertEqual(pool[0]["avg_confidence"], pool[1]["avg_confidence"])
        self.assertGreater(pool[0]["league_prior_boost"], 0)
        self.assertEqual(pool[1]["league_prior_boost"], 0)
        self.assertAlmostEqual(
            pool[0]["consensus_score"],
            pool[0]["consensus_base_score"] + pool[0]["league_prior_boost"],
            places=4,
        )

    def test_top_bounds_shortlist_requires_80_percent_and_50_matches(self):
        def pick(fid, rate, matches, confidence=0.82, odds=1.20):
            return {
                "fixture_id": fid,
                "sportybet_market": "Goal Bounds",
                "sportybet_selection": "1-4",
                "league_prior_rate": rate,
                "league_prior_matches": matches,
                "avg_confidence": confidence,
                "n_models": 1,
                "kickoff": f"2026-08-{fid + 10:02d}T18:00:00Z",
                "has_odds": odds is not None,
                "real_odds": odds,
            }

        shortlist = _build_bounds_1_4_shortlist([
            pick(1, 0.83, 500, 0.80),
            pick(2, 0.84, 100, 0.79),
            pick(3, 0.79, 900),
            pick(4, 0.90, 49),
            pick(5, 0.84, 500, odds=1.16),
            pick(6, 0.84, 500, odds=None),
        ])

        self.assertEqual([p["fixture_id"] for p in shortlist], [6, 2, 1])
        self.assertEqual([p["bounds_rank"] for p in shortlist], [1, 2, 3])
        self.assertEqual(
            [p["odds_status"] for p in shortlist],
            ["awaiting_goal_bounds_odds", "priced", "priced"],
        )

    def test_top_bounds_shortlist_caps_each_league_at_seven(self):
        ranked = []
        for fid in range(1, 19):
            ranked.append({
                "fixture_id": fid,
                "sportybet_market": "Goal Bounds",
                "sportybet_selection": "1-4",
                "league_prior_rate": 0.84 if fid <= 10 else 0.82,
                "league_prior_matches": 500,
                "avg_confidence": 0.82,
                "n_models": 1,
                "kickoff": f"2026-08-{fid + 1:02d}T18:00:00Z",
                "has_odds": True,
                "real_odds": 1.20,
                "league": "League A" if fid <= 10 else "League B",
                "country": "Testland",
            })

        shortlist = _build_bounds_1_4_shortlist(ranked)

        self.assertEqual(len(shortlist), 14)
        self.assertEqual(
            sum(p["league"] == "League A" for p in shortlist), 7)
        self.assertEqual(
            sum(p["league"] == "League B" for p in shortlist), 7)
        self.assertEqual(
            [p["bounds_rank"] for p in shortlist], list(range(1, 15)))

    def test_top_over_1_5_shortlist_applies_all_caps_and_floors(self):
        def pick(fid, league, belief=0.78, odds=1.17):
            return {
                "fixture_id": fid,
                "sportybet_market": "Over/Under",
                "sportybet_selection": "Over 1.5",
                "avg_confidence": belief,
                "max_confidence": belief,
                "n_models": 2,
                "kickoff": f"2026-09-{fid:02d}T18:00:00Z",
                "has_odds": odds is not None,
                "real_odds": odds,
                "league": league,
                "country": "Testland",
            }

        ranked = [pick(fid, "League A") for fid in range(1, 11)]
        ranked += [pick(fid, "League C") for fid in range(11, 16)]
        ranked += [
            pick(16, "League B"),
            pick(17, "League A", belief=0.74),
            pick(18, "League A", odds=1.16),
            pick(19, "League A", odds=None),
        ]
        rates = {
            ("League A", "Testland"): {"rate": 0.85, "matches": 500},
            ("League B", "Testland"): {"rate": 0.79, "matches": 500},
            ("League C", "Testland"): {"rate": 0.82, "matches": 100},
        }

        shortlist = _build_over_1_5_shortlist(ranked, rates)

        self.assertEqual(len(shortlist), 12)
        self.assertEqual(sum(p["league"] == "League A" for p in shortlist), 7)
        self.assertEqual(sum(p["league"] == "League C" for p in shortlist), 5)
        self.assertTrue(all(p["real_odds"] >= 1.17 for p in shortlist))
        self.assertTrue(all(p["avg_confidence"] >= 0.75 for p in shortlist))
        self.assertEqual(
            [p["over_1_5_rank"] for p in shortlist], list(range(1, 13)))


if __name__ == "__main__":
    unittest.main()
