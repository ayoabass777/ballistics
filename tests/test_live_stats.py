"""Full-time match statistics from the live poller's by-id fetch (etl/src/live_stats.py)."""
from datetime import datetime, timezone
import unittest

from etl.src import live_poller as lp
from etl.src import live_stats as st

NOW = datetime(2026, 11, 16, 21, 0, tzinfo=timezone.utc)


def item(api_id=500, status="FT", stats=True):
    blocks = [
        {"team": {"id": 10}, "statistics": [{"type": "Corner Kicks", "value": 2}, {"type": "Ball Possession", "value": "57%"},
                                            {"type": "Passes %", "value": "81%"}, {"type": "expected_goals", "value": "1.34"},
                                            {"type": "Goalkeeper Saves", "value": None}]},
        {"team": {"id": 20}, "statistics": [{"type": "Corner Kicks", "value": 6}, {"type": "Ball Possession", "value": "43%"}]},
        {"team": {"id": 99}, "statistics": [{"type": "Corner Kicks", "value": 1}]},   # not in this fixture
    ]
    return {"fixture": {"id": api_id, "status": {"short": status, "elapsed": 90, "extra": None}},
            "teams": {"home": {"id": 10}, "away": {"id": 20}}, "goals": {"home": 2, "away": 1},
            "events": [], "statistics": blocks if stats else []}


class ParseTests(unittest.TestCase):
    def test_keys_and_values(self):
        self.assertEqual(st.stat_key("Corner Kicks"), "corner_kicks")
        self.assertEqual(st.stat_key("Passes %"), "passes_pct")
        self.assertEqual(st.stat_key("Shots insidebox"), "shots_insidebox")
        self.assertEqual([st.stat_value(v) for v in (2, "57%", "1.34", None, "", True)], [2, 57.0, 1.34, None, None, None])

    def test_rows_by_side(self):
        rows = {(r["side"], r["stat"]): r for r in st.parse_statistics(item(), 7, NOW)}
        self.assertEqual(rows[("home", "corner_kicks")]["value"], 2)
        self.assertEqual(rows[("away", "corner_kicks")]["value"], 6)
        self.assertEqual((rows[("home", "ball_possession")]["value"], rows[("home", "ball_possession")]["raw_value"]), (57.0, "57%"))
        self.assertEqual(rows[("home", "expected_goals")]["value"], 1.34)
        self.assertIsNone(rows[("home", "goalkeeper_saves")]["raw_value"])
        self.assertEqual({r["fixture_id"] for r in rows.values()}, {7})
        self.assertEqual(len(rows), 7)   # the unknown team's block is dropped

    def test_no_statistics(self):
        self.assertEqual(st.parse_statistics(item(stats=False), 7, NOW), [])


class FakeProvider:
    def __init__(self, ids):
        self._ids, self.remaining = ids, 5000

    def live(self):
        return []

    def by_ids(self, api_ids):
        return self._ids


class FakeStore:
    def __init__(self, open_live, fail=False):
        self._open, self.fail, self.stats = open_live, fail, []

    def window(self, now): return []
    def open_fixtures(self): return self._open
    def next_kickoff(self, now): return None
    def fixture_ids(self, api_ids): return {}
    def write_poll(self, parsed, now): return [s["fixture_id"] for s, _ in parsed]

    def write_stats(self, rows):
        if self.fail:
            raise RuntimeError("db down")
        self.stats += rows
        return len(rows)


class PollerStatsTests(unittest.TestCase):
    def test_stores_statistics_for_finished_fixtures_only(self):
        store = FakeStore({7: 500, 8: 501})
        p = lp.Poller(FakeProvider([item(500), item(501, status="PST")]), store, lambda *a: None, clock=lambda: NOW)
        p.cycle()
        self.assertEqual({r["fixture_id"] for r in store.stats}, {7})

    def test_failed_statistics_write_does_not_stop_polling(self):
        p = lp.Poller(FakeProvider([item(500)]), FakeStore({7: 500}, fail=True), lambda *a: None, clock=lambda: NOW)
        self.assertEqual(p.cycle(), lp.POLL_SECONDS)
        self.assertEqual(p.failures, 0)


if __name__ == "__main__":
    unittest.main()
