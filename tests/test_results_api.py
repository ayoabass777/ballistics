"""Results API (engine/api/results.py)."""
from datetime import date, datetime, timezone
import unittest
from unittest.mock import Mock

from engine.api import results

NOW = datetime(2026, 11, 6, 12, tzinfo=timezone.utc)


def row(**over):
    base = {"fixture_id": 9, "fixture_status": "FT", "kickoff_utc": datetime(2026, 11, 5, 19, tzinfo=timezone.utc),
            "home_team_name": "Brann", "away_team_name": "Molde", "league_id": 3, "league_name": "Eliteserien",
            "country_name": "Norway", "home_team_fulltime_goal": 2, "away_team_fulltime_goal": 1,
            "home_team_halftime_goal": 1, "away_team_halftime_goal": 1, "home_logo_file_id": None,
            "away_logo_file_id": None, "league_logo_file_id": None}
    return {**base, **over}


class FakeCursor:
    def __init__(self, *results):
        self.results, self.params = list(results), []

    def execute(self, sql, params=None):
        self.params.append(params)

    def fetchone(self):
        return self.results.pop(0)

    def fetchall(self):
        return self.results.pop(0)


class ResultsApiTests(unittest.TestCase):
    def test_defaults_to_latest_day_with_results(self):
        cur = FakeCursor({"day": date(2026, 11, 5)}, {"previous_day": date(2026, 11, 4), "next_day": None}, [row()])
        body = results.results(cur, zone="Europe/Oslo", now=NOW)
        self.assertEqual((body["day"], body["previous_day"], body["next_day"]), ("2026-11-05", "2026-11-04", None))
        self.assertEqual(cur.params[0]["today"], date(2026, 11, 6))
        m = body["matches"][0]
        self.assertEqual((m["home_goals"], m["away_goals"], m["home_ht"], m["status"]), (2, 1, 1, "FT"))

    def test_empty_database(self):
        body = results.results(FakeCursor({"day": None}), now=NOW)
        self.assertEqual((body["day"], body["matches"]), (None, []))

    def test_rejects_future_day_and_bad_zone(self):
        with self.assertRaises(ValueError):
            results.results(FakeCursor(), day=date(2026, 11, 7), now=NOW)
        with self.assertRaises(ValueError):
            results.results(FakeCursor(), zone="Mars/Base")


class FixturesApiTests(unittest.TestCase):
    def test_combines_statuses_and_binds_inclusive_dates(self):
        cur = Mock()
        cur.fetchall.return_value = [row(fixture_status=status, fixture_id=i)
                                    for i, status in enumerate(["NS", "2H", "FT"], 1)]
        body = results.fixtures(cur, "America/Vancouver", date(2026, 11, 6), date(2026, 11, 8))
        self.assertEqual([m["status"] for m in body["matches"]], ["NS", "2H", "FT"])
        query, params = cur.execute.call_args.args
        self.assertNotIn("f.is_played", query)
        self.assertIn("BETWEEN %(date_from)s AND %(date_to)s", query)
        self.assertEqual(params["zone"], "America/Vancouver")

    def test_reversed_range_rejected_before_query(self):
        cur = Mock()
        with self.assertRaises(ValueError):
            results.fixtures(cur, "UTC", date(2026, 11, 8), date(2026, 11, 6))
        cur.execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
