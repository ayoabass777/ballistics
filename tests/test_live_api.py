"""Live scores API (engine/api/live.py)."""
from datetime import datetime, timezone
import unittest

from engine.api import live

T = datetime(2026, 11, 7, 15, 30, tzinfo=timezone.utc)


def row(**over):
    base = {"fixture_id": 7, "status": "2H", "elapsed": 63, "extra": None, "home_goals": 1,
            "away_goals": 0, "home_red_cards": 0, "away_red_cards": 1, "changed_at": T,
            "polled_at": T, "finished_at": None, "home_team_name": "Brann", "away_team_name": "Molde",
            "league_id": 3, "league_name": "Eliteserien", "country_name": "Norway",
            "kickoff_utc": T, "home_logo_file_id": None, "away_logo_file_id": None,
            "league_logo_file_id": None, "discovery": None}
    return {**base, **over}


class FakeCursor:
    def __init__(self, *results):
        self.results, self.calls = list(results), []

    def execute(self, sql, params=None):
        self.calls.append(sql)

    def fetchall(self):
        return self.results.pop(0)

    def fetchone(self):
        rows = self.results.pop(0)
        return rows[0] if rows else None


class LiveApiTests(unittest.TestCase):
    def setUp(self):
        live._list_cache.update(at=-live.LIST_TTL, body=None)

    def test_card_shape_and_leading_pick(self):
        discovery = {"sections": {
            "goals": {"list": "goals", "market": "Match Over 1.5", "probability": 0.84, "score": 0.3},
            "strength": {"list": "strength", "favourite": "Brann", "score": 0.6}}}
        body = live.live_list(FakeCursor([row(discovery=discovery)]), clock=lambda: 100.0)
        card = body["matches"][0]
        self.assertEqual((card["minute"], card["home_goals"], card["away_red_cards"], card["finished"]),
                         (63, 1, 1, False))
        self.assertEqual(card["pick"]["list"], "strength")
        self.assertNotIn("score", card["pick"])
        self.assertEqual(card["kickoff"], T.isoformat())

    def test_list_is_cached_briefly(self):
        cur = FakeCursor([row()], [row(fixture_id=8)])
        first = live.live_list(cur, clock=lambda: 100.0)
        self.assertIs(live.live_list(cur, clock=lambda: 103.0), first)
        self.assertEqual(live.live_list(cur, clock=lambda: 106.0)["matches"][0]["fixture_id"], 8)

    def test_fixture_not_seen_live(self):
        self.assertEqual(live.live_fixture(FakeCursor([]), 7), {"state": None, "events": []})

    def test_fixture_with_events(self):
        goal = {"minute": 20, "extra": None, "side": "home", "type": "Goal", "detail": "Normal Goal",
                "player": "X", "assist": None}
        body = live.live_fixture(FakeCursor([row(status="FT", finished_at=T)], [goal]), 7)
        self.assertTrue(body["state"]["finished"])
        self.assertEqual(body["events"], [goal])


if __name__ == "__main__":
    unittest.main()
