"""Per-market Dixon-Coles calibration (2026-10-03).

One shared DC map pulled every market to the same top value. Each market
family now has its own map, with the pooled map as fallback.
"""

import unittest

from engine.eval import predict_dixon_coles as dc
from engine.eval.calibration_maps import DC_MARKET_KEYS, DC_POOLED_KEY, dc_calibration_key
from engine.eval.calibration_wf import (
    CALIBRATION_KEYS,
    _fit_map,
    _calibration_key,
    _points_for,
    _rows_for_key,
)


class MarketRoutingTests(unittest.TestCase):
    def test_every_priced_market_has_its_own_key(self):
        params = {"mu": 0.15, "home_adv": 0.2, "rho": -0.05,
                  "attack": {1: 0.1, 2: -0.1}, "defence": {1: -0.05, 2: 0.05}}
        markets = dc.fixture_markets(params, 1, 2, "Union St. Gilloise", "OH Leuven")["markets"]
        keys = {m: dc_calibration_key(m) for m in markets}
        self.assertNotIn(DC_POOLED_KEY, keys.values(), keys)
        self.assertEqual(keys["Union St. Gilloise Over 0.5"], "dixon_coles_team_score")
        self.assertEqual(keys["OH Leuven Over 1.5"], "dixon_coles_team_over_1_5")
        self.assertEqual(keys["OH Leuven Win"], "dixon_coles_team_win")
        self.assertEqual(keys["Match Under 3.5"], "dixon_coles_under_3_5")
        self.assertEqual(keys["BTTS Yes"], "dixon_coles_btts")
        self.assertEqual(keys["Match Goals 1-4"], "dixon_coles_bounds_1_4")

    def test_unknown_market_uses_pooled_key(self):
        self.assertEqual(dc_calibration_key("Something new"), DC_POOLED_KEY)

    def test_every_dc_key_is_fitted(self):
        for key in DC_MARKET_KEYS:
            self.assertIn(key, CALIBRATION_KEYS)
        self.assertIn(DC_POOLED_KEY, CALIBRATION_KEYS)


class FittingRowsTests(unittest.TestCase):
    def setUp(self):
        self.preds = [
            {"model": "dixon_coles", "market": "A Over 0.5"},
            {"model": "dixon_coles", "market": "Match Under 3.5"},
            {"model": "dixon_coles", "market": "Match Goals 1-4"},
            {"model": "poisson", "market": "Match Over 1.5"},
        ]

    def test_pooled_map_uses_all_dc_except_goals_1_4(self):
        rows = _rows_for_key(self.preds, DC_POOLED_KEY)
        self.assertEqual([r["market"] for r in rows], ["A Over 0.5", "Match Under 3.5"])

    def test_family_map_uses_only_its_market(self):
        rows = _rows_for_key(self.preds, "dixon_coles_under_3_5")
        self.assertEqual([r["market"] for r in rows], ["Match Under 3.5"])

    def test_poisson_routing_unchanged(self):
        self.assertEqual(_calibration_key(self.preds[3]), "poisson")

    def test_empty_family_map_falls_back_to_pooled(self):
        maps = {DC_POOLED_KEY: [[0, 0.5], [1, 0.5]], "dixon_coles_btts": []}
        self.assertEqual(_points_for(maps, "dixon_coles_btts"), [[0, 0.5], [1, 0.5]])
        self.assertEqual(_points_for(maps, "poisson"), [])


class BinSizeTests(unittest.TestCase):
    def rows(self, n):
        return [{"predicted_prob_raw": i / n, "actual": i % 3 != 0} for i in range(n)]

    def test_market_maps_need_150_per_bin(self):
        self.assertEqual(_fit_map(self.rows(299), "dixon_coles_btts"), [])
        self.assertEqual(len(_fit_map(self.rows(600), "dixon_coles_btts")), 4)

    def test_pooled_and_goals_1_4_keep_default_bins(self):
        self.assertEqual(len(_fit_map(self.rows(600), "dixon_coles")), 10)
        self.assertEqual(len(_fit_map(self.rows(600), "dixon_coles_bounds_1_4")), 10)


class LiveCalibratorTests(unittest.TestCase):
    def setUp(self):
        self.saved = (dc._DC_POINTS, dc._DC_MARKET_POINTS, dc._DC_BOUNDS_1_4_POINTS)

    def tearDown(self):
        dc._DC_POINTS, dc._DC_MARKET_POINTS, dc._DC_BOUNDS_1_4_POINTS = self.saved

    def test_market_map_beats_pooled_and_is_recorded(self):
        dc._DC_POINTS = [[0.0, 0.8387], [1.0, 0.8387]]
        dc._DC_MARKET_POINTS = {"dixon_coles_team_score": [[0.0, 0.9], [1.0, 0.9]]}
        self.assertAlmostEqual(dc._calibrate(0.95, "Barcelona Over 0.5"), 0.9)
        self.assertEqual(dc._points_for_market("Barcelona Over 0.5")[1], "dixon_coles_team_score")

    def test_missing_market_map_falls_back_to_pooled(self):
        dc._DC_POINTS = [[0.0, 0.7], [1.0, 0.7]]
        dc._DC_MARKET_POINTS = {}
        self.assertAlmostEqual(dc._calibrate(0.95, "Match Under 2.5"), 0.7)
        self.assertEqual(dc._points_for_market("Match Under 2.5")[1], DC_POOLED_KEY)

    def test_no_maps_is_identity(self):
        dc._DC_POINTS, dc._DC_MARKET_POINTS, dc._DC_BOUNDS_1_4_POINTS = None, {}, None
        self.assertAlmostEqual(dc._calibrate(0.95, "Match Over 1.5"), 0.95)
        self.assertEqual(dc._points_for_market("Match Over 1.5")[1], "identity")


if __name__ == "__main__":
    unittest.main()
