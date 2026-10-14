"""Match page Context: standings position and H2H summary (contract §3.2, §5)."""
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from engine.api.app import app
from engine.api import forecast_records as records
from engine.api import match_context as mc


def row(team_id, rank, points, group="Regular Season", order=1, current=True, repeat=False, n_groups=1):
    return {"team_id": team_id, "team_name": f"T{team_id}", "group_name": group, "group_order": order,
            "rank": rank, "games_played": 9, "wins": 1, "draws": 1, "losses": 7, "goals_for": 5,
            "goals_against": 19, "goal_diff": -14, "points": points, "form": "LLLDL",
            "description": None, "standings_updated_at": datetime(2026, 10, 5, tzinfo=timezone.utc),
            "standings_format": "single", "n_groups": n_groups, "ranks_repeat": repeat,
            "is_team_current_group": current}


def meet(day, is_home, gf, ga):
    return {"fixture_id": day, "kickoff_utc": datetime(2024, 1, day, tzinfo=timezone.utc),
            "is_home": is_home, "goals_for": gf, "goals_against": ga,
            "team_name": "Zlin", "opponent_name": "Slavia"}


class PositionTests(unittest.TestCase):
    def test_team_in_table(self):
        p = mc.team_position([row(1, 16, 1), row(2, 1, 23)], 1)
        self.assertEqual((p["rank"], p["points"], p["group_size"]), (16, 1, 2))

    def test_missing_team_is_none_not_zero(self):
        self.assertIsNone(mc.team_position([row(2, 1, 23)], 1))
        self.assertIsNone(mc.team_position([row(2, 1, 23)], None))

    def test_split_season_uses_current_group(self):
        rows = [row(1, 3, 30, "Regular Season", 1, current=False, n_groups=2),
                row(1, 1, 35, "Championship Round", 2, n_groups=2)]
        p = mc.team_position(rows, 1)
        self.assertEqual((p["group_name"], p["rank"], p["show_group_name"]), ("Championship Round", 1, True))

    def test_repeated_ranks_hide_position(self):
        self.assertIsNone(mc.team_position([row(1, 1, 0, repeat=True)], 1)["rank"])


class H2HTests(unittest.TestCase):
    # Zlin v Slavia worked example: 1-3, 0-0, 1-1, 2-1(Slavia home), 0-4.
    GAMES = [meet(5, True, 1, 3), meet(4, False, 0, 0), meet(3, True, 1, 1),
             meet(2, False, 1, 2), meet(1, True, 0, 4)]

    def test_worked_example(self):
        s = mc.summarise_h2h(self.GAMES, "Zlin", "Slavia")
        self.assertEqual(s["tally"], {"home_wins": 0, "draws": 2, "away_wins": 3})
        self.assertEqual(s["stats"]["goals_per_game"], 2.6)
        self.assertEqual(s["stats"]["over_2_5"], 3)
        self.assertEqual(s["stats"]["both_scored"], 3)
        self.assertEqual(s["stats"]["at_home_team"], {"meetings": 3, "home_wins": 0, "draws": 1, "away_wins": 2})
        g = s["games"][3]
        self.assertEqual((g["home"], g["home_goals"], g["away_goals"], g["winner"]), ("Slavia", 2, 1, "Slavia"))

    def test_few_meetings_no_goal_stats(self):
        s = mc.summarise_h2h(self.GAMES[:2], "Zlin", "Slavia")
        self.assertIsNone(s["stats"])
        self.assertEqual(s["stats_note"], "too few for goal stats")

    def test_no_meetings(self):
        s = mc.summarise_h2h([], "Zlin", "Slavia")
        self.assertEqual(s["meetings"], 0)
        self.assertEqual(s["message"], "No previous meetings in our records")


class FakeCursor:
    def __init__(self, results):
        self.results, self.calls = list(results), []
    def execute(self, sql, params):
        self.calls.append(params)
    def fetchone(self):
        return self.results.pop(0)
    def fetchall(self):
        return self.results.pop(0)


class BuildTests(unittest.TestCase):
    FX = {"fixture_id": 90863, "league_season_id": 7, "league_id": 3, "league_name": "Czech Liga",
          "country_name": "Czech-Republic", "kickoff_utc": datetime(2026, 10, 9, 16, tzinfo=timezone.utc),
          "home_team_id": 1, "away_team_id": 2, "home_team_name": "Zlin", "away_team_name": "Slavia",
          "home_logo_file_id": "u1", "away_logo_file_id": None}

    def test_payload(self):
        cur = FakeCursor([self.FX, [row(1, 16, 1), row(2, 1, 23)], H2HTests.GAMES])
        p = mc.build_context(cur, 90863)
        self.assertEqual(p["position"]["points_gap"], 22)
        self.assertEqual(p["h2h"]["meetings"], 5)
        self.assertEqual(cur.calls[2]["cutoff"], self.FX["kickoff_utc"])
        self.assertTrue(p["table"]["available"])
        self.assertTrue(p["fixture"]["home_crest_url"].endswith("/crests/teams/u1"))
        self.assertIsNone(p["fixture"]["away_crest_url"])

    def test_unknown_fixture(self):
        self.assertIsNone(mc.build_context(FakeCursor([None]), 1))

    def test_unmapped_team_skips_h2h_query(self):
        fx = dict(self.FX, away_team_id=None)
        cur = FakeCursor([fx, [row(1, 16, 1)]])
        p = mc.build_context(cur, 90863)
        self.assertIsNone(p["position"]["away"])
        self.assertEqual(p["h2h"]["meetings"], 0)
        self.assertEqual(len(cur.calls), 2)


class HTTPTests(unittest.TestCase):
    def test_bad_id_does_not_connect(self):
        with TestClient(app) as client, patch.object(records, "connect") as connect:
            self.assertEqual(client.get("/api/match/0/context").status_code, 422)
            connect.assert_not_called()

    def test_unavailable_does_not_leak(self):
        with TestClient(app) as client, patch.object(records, "connect", side_effect=RuntimeError("secret-dsn")):
            r = client.get("/api/match/90863/context")
            self.assertEqual(r.status_code, 503)
            self.assertNotIn("secret", r.text)


if __name__ == "__main__":
    unittest.main()
