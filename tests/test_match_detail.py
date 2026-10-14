"""Full match page payload (engine.api.match_detail)."""
from datetime import datetime, timezone
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from engine.api import forecast_records as records
from engine.api import match_detail as md
from engine.api.app import app

STRENGTH = {"home_team": {"overall": {"attack_rate": 0.9, "defense_rate": 1.6, "attack_shrunk": 1.05,
                                      "attack_pct": 21.9, "attack_rank": 14, "defense_rank": 12,
                                      "league_size": 16}},
            "away_team": {"overall": {"attack_rate": 0.8, "defense_rate": 2.0, "attack_rank": 15,
                                      "defense_rank": 15, "league_size": 16}}}


def pattern(side, event, k, n, base, team):
    a, b = 15 * base + k, 15 * (1 - base) + n - k
    return {"side": side, "team": team, "event": event, "k": k, "n": n, "alpha": a, "beta": b,
            "base_rate": base, "league_gap": round(k / n - base, 4), "league_comparison": "well_above",
            "display_eligible": False, "context": "all", "primary": True, "streak": 3,
            "posterior": 0.62}


FIXTURE = {
    "fixture_id": 90876, "home": "Pardubice", "away": "Zlin", "league": "Czech Liga",
    "country": "Czech-Republic", "home_team_id": 26815, "away_team_id": 26802, "league_id": 233,
    "strength": STRENGTH,
    "mismatch": [{"market": "Pardubice Over 0.5", "probability": 0.81, "gap": 136}],
    "dixon_coles": [
        {"market": "Match Goals 1-4", "probability": 0.8032, "raw": 0.79, "league_rate": 0.8481,
         "league_rate_context": "all", "league_comparison": "typical", "league_gap": -0.04},
        {"market": "Pardubice Win", "probability": 0.55, "league_comparison": "above"},
        {"market": "Pardubice Over 0.5", "probability": 0.7969, "league_rate": 0.7342,
         "league_rate_context": "home", "league_comparison": "above"},
        {"market": "Match Over 1.5", "probability": 0.70, "league_rate": 0.8,
         "league_rate_context": "all", "league_comparison": "too_few_matches"},
        {"market": "BTTS", "probability": 0.5, "league_comparison": "typical"},
    ],
    "bayesian": [pattern("away", "loss", 9, 10, 0.40, "Zlin")],
    "discover_pick": {"list": "patterns", "score": 0.2, "team": "Zlin", "event": "loss",
                      "shrunk_rate": 0.59},
}

EXPORT_RAW = {
    "run_ds": "2026-10-13",
    "fixture_analysis": [FIXTURE],
    "elo_gap_shortlist": [{"fixture_id": 60898, "favourite": "Rigas FS", "underdog": "Tukums",
                           "elo_gap": 467, "league_big_dog_won": 267, "league_big_dog_matches": 322}],
    "elo_gap_group": {"gap_min": 250, "big_dog_leagues": [
        {"league": "Czech Liga", "country": "Czech-Republic", "status": "in", "reason": "low ≥ 60%"},
        {"league": "Liga 2", "country": "X", "status": "out", "reason": "tier 2 (top flight only)"},
        {"league": "Small", "country": "Y", "status": "out", "reason": "< 30 Big dog matches"},
        {"league": "Wild", "country": "Z", "status": "out", "reason": "low < 60%"}]},
}


def context_payload(fixture_id=90876):
    return {"fixture": {"fixture_id": fixture_id, "league_id": 233, "league": "Czech Liga",
                        "country": "Czech-Republic", "kickoff_utc": "2026-10-17T13:00:00+00:00",
                        "home": "Pardubice", "away": "Zlin", "home_team_id": 26815,
                        "away_team_id": 26802, "home_crest_url": "https://x/crests/teams/a",
                        "away_crest_url": None},
            "standings_updated_at": "2026-10-12T00:00:00+00:00",
            "position": {"home": None, "away": None, "points_gap": None},
            "table": {"available": False, "standings_format": None, "groups": []},
            "h2h": {"meetings": 0, "games": []}}


class SectionTests(unittest.TestCase):
    def setUp(self):
        self.export = md.index_export(EXPORT_RAW)

    def test_strength_states(self):
        f = dict(FIXTURE)
        self.assertEqual(md.strength_section(f, self.export), {"state": "close"})
        self.assertEqual(md.strength_section(dict(f, mismatch=[]), self.export), {"state": "close"})
        self.assertIsNone(md.strength_section(dict(f, mismatch=[], strength=None), self.export))
        big = dict(f, fixture_id=60898)
        self.assertEqual(md.strength_section(big, self.export),
                         {"state": "big_dog", "favourite": "Rigas FS", "underdog": "Tukums"})
        wide = [{"gap": 300}]
        for league, country, why in (("Liga 2", "X", "top_flight_only"),
                                     ("Small", "Y", "not_enough_matches"),
                                     ("Wild", "Z", "unpredictable")):
            s = md.strength_section(dict(f, mismatch=wide, league=league, country=country), self.export)
            self.assertEqual(s, {"state": "no_label", "reason": why})

    def test_strength_never_exposes_counts_or_gap(self):
        s = md.strength_section(dict(FIXTURE, fixture_id=60898), self.export)
        self.assertEqual(set(s), {"state", "favourite", "underdog"})

    def test_goals(self):
        g = md.goals_section(FIXTURE)
        self.assertEqual([r["market"] for r in g["rows"]],
                         ["Match Goals 1-4", "Pardubice Over 0.5", "Match Over 1.5"])
        self.assertNotIn("raw", g["rows"][0])
        self.assertIsNone(g["rows"][2]["league_rate"])          # too few league matches
        self.assertFalse(g["nothing_stands_out"])
        self.assertIsNone(md.goals_section(dict(FIXTURE, dixon_coles=[])))

    def test_form_rows_drop_internal_rates(self):
        rows = md.form_rows(STRENGTH)
        self.assertEqual(rows["home_team"]["overall"]["attack_rank"], 14)
        self.assertNotIn("attack_shrunk", rows["home_team"]["overall"])
        self.assertNotIn("attack_pct", rows["home_team"]["overall"])
        self.assertIsNone(rows["home_team"]["home"])

    def test_patterns_computed_for_older_exports(self):
        p = md.patterns_section(FIXTURE)
        self.assertEqual(p["all"]["top_results"][0]["label"], "Lost")
        self.assertNotIn("posterior", p["all"]["top_results"][0])

    def test_discover_hides_scores(self):
        self.assertEqual(md.discover_section(FIXTURE), {"list": "patterns", "team": "Zlin", "event": "loss"})


class LoadTests(unittest.TestCase):
    def test_latest_export_by_date_and_cache(self):
        with tempfile.TemporaryDirectory() as d:
            for ds in ("2026-10-12", "2026-10-13"):
                Path(d, f"consensus_slips_{ds}.json").write_text(json.dumps(dict(EXPORT_RAW, run_ds=ds)))
            Path(d, "consensus_slips_2026-10-14_draft.json").write_text("{}")
            path = md.latest_export_path(Path(d))
            self.assertEqual(path.name, "consensus_slips_2026-10-13.json")
            self.assertEqual(md.load_export(path)["run_ds"], "2026-10-13")
            self.assertIs(md.load_export(path), md.load_export(path))

    def test_no_export_dir(self):
        self.assertIsNone(md.latest_export_path(Path("/nonexistent")))


class FakeCursor:
    """Answers the two engine-store queries; Context is patched separately."""
    def __init__(self, ready=True, row=None):
        self.ready, self.row, self.calls = ready, row, []
        self._next = None
    def execute(self, sql, params=None):
        self.calls.append(sql)
        self._next = {"ready": self.ready} if "to_regclass" in sql else self.row
    def fetchone(self):
        return self._next


def stored_row(fid=90876):
    export = md.index_export(EXPORT_RAW)
    payload = json.loads(json.dumps(md.engine_sections(export["fixtures"][fid], export)))
    return {"run_ds": datetime(2026, 10, 13).date(),
            "finished_at": datetime(2026, 10, 14, 7, 5, tzinfo=timezone.utc), "payload": payload}


class BuildTests(unittest.TestCase):
    def build(self, cur, fid=90876):
        with patch.object(md.match_context, "build_context", return_value=context_payload(fid)):
            return md.build_match(cur, fid)

    def test_full_payload_from_store(self):
        p = self.build(FakeCursor(row=stored_row()))
        self.assertEqual(p["fixture"]["run_ds"], "2026-10-13")
        self.assertEqual(p["fixture"]["run_finished_at"], "2026-10-14T07:05:00+00:00")
        self.assertEqual(p["fixture"]["standings_updated_at"], "2026-10-12T00:00:00+00:00")
        self.assertEqual(p["strength"], {"state": "close"})
        self.assertEqual(p["patterns"]["all"]["top_results"][0]["label"], "Lost")
        self.assertEqual(p["context"]["form"]["away_team"]["overall"]["league_size"], 16)
        self.assertIn("h2h", p["context"])
        text = json.dumps(p)
        for secret in ("alpha", "posterior", "shrunk", "elo_gap", "league_confidence", "\"gap\""):
            self.assertNotIn(secret, text)

    def test_fixture_not_in_latest_run_keeps_context(self):
        row = dict(stored_row(), payload=None)          # latest run exists, fixture absent
        p = self.build(FakeCursor(row=row), fid=1)
        self.assertIsNone(p["fixture"]["run_ds"])
        self.assertIsNone(p["strength"])
        self.assertIsNone(p["patterns"])
        self.assertIsNotNone(p["context"]["h2h"])

    def test_store_not_created_or_empty(self):
        for cur in (FakeCursor(ready=False), FakeCursor(row=None)):
            p = self.build(cur)
            self.assertIsNone(p["goals"])
            self.assertIsNone(p["fixture"]["run_ds"])

    def test_only_latest_complete_run_is_read(self):
        sql = md.LATEST_SQL
        self.assertIn("status = 'complete'", sql)
        self.assertIn("ORDER BY run_ds DESC LIMIT 1", sql)

    def test_unknown_fixture(self):
        with patch.object(md.match_context, "build_context", return_value=None):
            self.assertIsNone(md.build_match(FakeCursor(), 5))


class HTTPTests(unittest.TestCase):
    def test_bad_id(self):
        with TestClient(app) as client, patch.object(records, "connect") as connect:
            self.assertEqual(client.get("/api/match/0").status_code, 422)
            connect.assert_not_called()

    def test_db_down_does_not_leak(self):
        with TestClient(app) as client, \
                patch.object(records, "connect", side_effect=RuntimeError("secret-dsn")):
            r = client.get("/api/match/90876")
            self.assertEqual(r.status_code, 503)
            self.assertNotIn("secret", r.text)

    def test_serves_from_store(self):
        with TestClient(app) as client, \
                patch.object(records, "read_cursor") as rc, \
                patch.object(md.match_context, "build_context", return_value=context_payload()):
            rc.return_value.__enter__.return_value = FakeCursor(row=stored_row())
            r = client.get("/api/match/90876")
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()["strength"], {"state": "close"})

if __name__ == "__main__":
    unittest.main()
