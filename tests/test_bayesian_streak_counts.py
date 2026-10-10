"""Regression coverage for season/context-scoped consecutive event counts."""

import unittest

import pandas as pd

from engine.eval.bayesian_streaks import compute_beliefs, apply_streak_boost


class BayesianStreakCountTests(unittest.TestCase):
    def belief(self, flags, context="home"):
        history = pd.DataFrame({
            "team_id": [1] * len(flags),
            "team_name": ["Example FC"] * len(flags),
            "league": ["Example League"] * len(flags),
            "event_name": ["score_1goal"] * len(flags),
            "event_flag": flags,
        })
        rates = pd.DataFrame([{
            "league": "Example League", "event_name": "score_1goal",
            "base_rate": 0.6,
        }])
        return compute_beliefs(history, rates, context=context)[0]

    def test_full_trailing_run_beyond_ten_in_each_context(self):
        for context in ("home", "away", "all"):
            with self.subTest(context=context):
                belief = self.belief([1, 1, 0] + [1] * 12, context)
                self.assertEqual(belief.recent_streak, 12)
                self.assertEqual(belief.context, context)
                self.assertEqual(belief.k_matches, 14)
                self.assertEqual(belief.n_matches, 15)
                self.assertEqual(belief.posterior, round((9 + 14) / 30, 4))

    def test_unbroken_season_run(self):
        self.assertEqual(self.belief([1] * 18).recent_streak, 18)

    def test_latest_miss_resets_run(self):
        self.assertEqual(self.belief([1] * 12 + [0]).recent_streak, 0)

    def test_short_and_exactly_ten_runs(self):
        for length in (1, 3, 10):
            with self.subTest(length=length):
                self.assertEqual(
                    self.belief([1, 0] + [1] * length).recent_streak, length)

    def test_longer_counts_do_not_increase_capped_boost(self):
        self.assertEqual(apply_streak_boost(0.8, 10), apply_streak_boost(0.8, 18))


if __name__ == "__main__":
    unittest.main()
