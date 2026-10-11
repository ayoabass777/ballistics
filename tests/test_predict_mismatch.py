import unittest
from unittest import mock
from pathlib import Path

import pandas as pd

from engine.eval import predict_mismatch
from engine.eval.predict_mismatch import _calibrate_conf, _lookup_hit_rate


def _rate_row(api_league_id, country, over15_pct, n=50):
    return {
        "api_league_id": api_league_id,
        "league": "Premier League" if api_league_id is not None else "__global__",
        "country": country,
        "gap_bin": "100-200",
        "n": n,
        "fav_scores_pct": 0.8,
        "fav_over15_pct": 0.5,
        "over15_pct": over15_pct,
        "over25_pct": 0.5,
        "fav_win_pct": 0.6,
        "draw_pct": 0.25,
    }


class LeagueAwareMismatchRatesTests(unittest.TestCase):
    def setUp(self):
        self.rates = pd.DataFrame([
            _rate_row(39, "England", 0.91),
            _rate_row(389, "Kazakhstan", 0.74, n=80),
            _rate_row(None, "__global__", 0.79, n=1000),
        ])

    def test_uses_api_league_id_when_names_collide(self):
        confidence, n = _lookup_hit_rate(
            self.rates,
            "Premier League",
            "100-200",
            "over_1.5",
            api_league_id=389,
        )

        self.assertEqual(confidence, 0.74)
        self.assertEqual(n, 80)

    def test_unknown_league_id_falls_back_to_global(self):
        confidence, n = _lookup_hit_rate(
            self.rates,
            "Premier League",
            "100-200",
            "over_1.5",
            api_league_id=9999,
        )

        self.assertEqual(confidence, 0.79)
        self.assertEqual(n, 1000)

    def test_ambiguous_name_only_lookup_does_not_pick_first_league(self):
        confidence, n = _lookup_hit_rate(
            self.rates,
            "Premier League",
            "100-200",
            "over_1.5",
        )

        self.assertEqual(confidence, 0.79)
        self.assertEqual(n, 1000)

    def test_live_calibration_applies_matching_league_adjustment(self):
        with mock.patch.object(predict_mismatch, "_MISMATCH_POINTS", None), \
                mock.patch.object(
                    predict_mismatch,
                    "_MISMATCH_LEAGUE_ADJUSTMENTS",
                    {"389": {"logit_delta": -0.3}},
                ):
            global_only = _calibrate_conf(0.8, api_league_id=39)
            kazakhstan = _calibrate_conf(0.8, api_league_id=389)

        self.assertEqual(global_only, 0.8)
        self.assertLess(kazakhstan, global_only)


class MismatchDagWiringTests(unittest.TestCase):
    def test_daily_dag_refreshes_elo_before_mismatch(self):
        source = Path("airflow/dags/engine_predict_dag.py").read_text()

        self.assertIn('task_id="refresh_elo"', source)
        self.assertIn('command=["python", "-m", "engine.elo.elo_service"]', source)
        self.assertIn(
            'wait_for_dbt_refresh >> refresh_elo >> model_tasks["mismatch"]',
            source,
        )

    def test_engine_waits_for_dbt_not_just_xg(self):
        # The engine reads dbt facts; it must start after fixture_update's dbt run.
        source = Path("airflow/dags/engine_predict_dag.py").read_text()
        self.assertIn('external_dag_id="fixture_update_dag"', source)
        self.assertIn('external_task_id="run_dbt"', source)
        self.assertNotIn('external_task_id="extract_xg_task"', source)

    def test_dixon_coles_reads_xg_from_dbt_fact(self):
        source = Path("engine/eval/predict_dixon_coles.py").read_text()
        self.assertIn('XG_TABLE = "raw.fact_fixture_xg"', source)
        self.assertNotIn("FROM raw.fixture_xg", source)

    def test_daily_dag_verifies_all_shortlists_after_consensus(self):
        source = Path("airflow/dags/engine_predict_dag.py").read_text()

        self.assertIn('task_id="verify_daily_shortlists"', source)
        self.assertIn(
            '"engine.eval.validate_daily_shortlists"',
            source,
        )
        self.assertIn(
            'gate >> consensus >> verify_daily_shortlists',
            source,
        )


if __name__ == "__main__":
    unittest.main()
