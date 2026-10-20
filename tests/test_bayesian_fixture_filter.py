"""Bayesian forecasts not-started fixtures only, like Mismatch and Dixon-Coles.

Regression: a "not finished" filter (NOT IN FT/AET/PEN) let postponed,
cancelled, orphaned and live fixtures through; on 2026-10-19 18 of them got
match pages and Discover cards (cancelled fixture 61899 among them).
"""
import re
import unittest
from unittest.mock import patch

import pandas as pd

from engine.eval import bayesian_streaks as bs


class FixtureStatusFilterTests(unittest.TestCase):
    def capture_sql(self, call):
        seen = []

        def fake_read_sql(sql, conn, params=None):
            seen.append(sql)
            return pd.DataFrame()

        with patch.object(bs.pd, "read_sql", side_effect=fake_read_sql):
            call()
        return seen[0]

    def assert_not_started_only(self, sql):
        self.assertNotIn("NOT IN ('FT'", sql)
        self.assertTrue(re.search(r"fixture_status\s*=\s*'NS'", sql), sql)

    def test_fixture_beliefs_select_not_started_fixtures(self):
        sql = self.capture_sql(lambda: bs.get_fixture_beliefs(conn=None, days=7))
        self.assert_not_started_only(sql)

    def test_upcoming_beliefs_select_not_started_fixtures(self):
        sql = self.capture_sql(lambda: bs.get_upcoming_beliefs(conn=None, days=7))
        self.assert_not_started_only(sql)
        self.assertEqual(sql.count("fixture_status = 'NS'"), 2)   # home and away sides


if __name__ == "__main__":
    unittest.main()
