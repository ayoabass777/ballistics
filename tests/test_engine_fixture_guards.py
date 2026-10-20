"""Guards against forecasting fixtures the engine knows nothing usable about.

- Mismatch skips a fixture when either team has no Elo history: the old 1500
  default made up a gap (and confident picks) against an unknown team.
- Consensus keeps not-started fixtures only, as a backstop for any model that
  lets a postponed/cancelled/live fixture through (Bayesian did, 2026-10-19).
"""
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd

from engine.eval import consensus_slips as cs
from engine.eval import predict_mismatch as pm


def _hit_rates():
    row = {"api_league_id": 1, "league": "Test League", "country": "Testland",
           "gap_lo": 200, "gap_hi": 300, "gap_bin": "200-300", "n": 500,
           "fav_scores_pct": 0.9, "fav_over15_pct": 0.6, "over15_pct": 0.8,
           "over25_pct": 0.6, "fav_win_pct": 0.8, "draw_pct": 0.15,
           "avg_fav_goals": 2.0, "avg_total": 3.0}
    return pd.DataFrame([row, {**row, "api_league_id": None, "league": "__global__",
                               "country": "__global__"}])


class MismatchUnratedTeamTests(unittest.TestCase):
    def run_with_elo(self, elo):
        kickoff = datetime.now(timezone.utc) + timedelta(days=1)
        upcoming = pd.DataFrame([{
            "fixture_id": 10, "kickoff_utc": kickoff, "home_team_id": 1,
            "home_team_name": "Rated FC", "away_team_id": 2,
            "away_team_name": "Promoted FC", "api_league_id": 1}])
        with patch.object(pm, "_get_conn", return_value=MagicMock()), \
                patch.object(pm, "build_hit_rates", return_value=_hit_rates()), \
                patch.object(pm.pd, "read_sql", return_value=upcoming), \
                patch.object(pm, "_load_league_map", return_value={1: "Test League"}), \
                patch.object(pm, "_load_league_country_map", return_value={1: "Testland"}), \
                patch.object(pm, "_get_latest_elo", return_value=elo), \
                patch.object(pm, "_load_h2h_records", return_value={}), \
                patch.object(pm, "_load_team_strength", return_value={}):
            picks, _ = pm.predict_mismatch(days=7)
        return picks

    def test_unrated_team_gets_no_picks(self):
        # Before: the away side defaulted to 1500, a 220 gap, and fav picks.
        self.assertEqual(self.run_with_elo({1: 1720.0}), [])

    def test_rated_teams_still_get_picks(self):
        picks = self.run_with_elo({1: 1720.0, 2: 1500.0})
        self.assertTrue(picks)
        self.assertEqual({p.favourite for p in picks}, {"Rated FC"})


class ConsensusStatusBackstopTests(unittest.TestCase):
    def test_fixture_meta_keeps_not_started_fixtures_only(self):
        rows = pd.DataFrame([
            {"fixture_id": fid, "home": f"H{fid}", "away": f"A{fid}",
             "kickoff": "2026-10-25 15:00:00+00", "api_league_id": 1,
             "home_team_id": 1, "away_team_id": 2, "league_id": 1,
             "fixture_status": status}
            for fid, status in ((1, "NS"), (2, "PST"), (3, "CANC"), (4, "ORPHAN"), (5, "1H"))])
        with patch.object(cs, "_league_id_country_map",
                          return_value=({1: "Test League"}, {1: "Testland"})), \
                patch.object(cs.pd, "read_sql", return_value=rows):
            meta = cs.fixture_meta_from_db([1, 2, 3, 4, 5], conn=MagicMock())
        self.assertEqual(set(meta), {1})


if __name__ == "__main__":
    unittest.main()
