"""Pure rules plus opt-in integration on a dedicated forecast_test* database."""
import copy
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from engine.eval import forecast_history as fh


NOW = datetime(2026, 10, 2, 12, tzinfo=timezone.utc)


def payload(kickoff=None):
    return {"run_ds": "2026-10-02", "fixture_analysis": [{
        "fixture_id": 1, "home": "Home", "away": "Away",
        "kickoff": (kickoff or NOW + timedelta(hours=2)).isoformat(),
        "dixon_coles": [{"market": "Match Over 1.5", "probability": .8, "raw": .75}]
    }]}


class ForecastRulesTests(unittest.TestCase):
    def test_normalises_goal_events_not_strength(self):
        for market, key in [("Match Goals 1-4", "match:bounds:1:4"),
                            ("Match Under 2.5", "match:under:2.5"),
                            ("Home Over 0.5", "home:over:0.5"),
                            ("Away Under 1.5", "away:under:1.5"),
                            ("BTTS Yes", "btts:yes")]:
            self.assertEqual(fh.event_key(market, "Home", "Away"), key)
        self.assertIsNone(fh.event_key("Home Win", "Home", "Away"))
        self.assertIsNone(fh.event_key("Match Over 2.0", "Home", "Away"))

    def test_invalid_or_duplicate_forecasts_fail(self):
        for prob in (None, -1, 1.01, float("nan"), float("inf")):
            p = payload()
            p["fixture_analysis"][0]["dixon_coles"][0]["probability"] = prob
            with self.assertRaises((ValueError, TypeError)):
                fh.extract_entries(p)
        p = payload()
        p["fixture_analysis"][0]["dixon_coles"] *= 2
        with self.assertRaisesRegex(ValueError, "duplicate"):
            fh.extract_entries(p)

    def test_timezone_is_required(self):
        with self.assertRaises(ValueError):
            fh.utc("2026-10-02T12:00:00")
        self.assertEqual(fh.utc("2026-10-02T05:00:00-07:00"), NOW)

    def test_capture_eligibility_is_strict(self):
        entry = fh.extract_entries(payload())[0]
        fx = {"kickoff_utc": entry["kickoff"], "fixture_status": "NS"}
        self.assertTrue(fh.eligibility(entry, fx, NOW)[0])
        self.assertFalse(fh.eligibility(entry, fx, entry["kickoff"])[0])
        self.assertFalse(fh.eligibility(entry, None, NOW)[0])
        for status in ("FT", "1H", "PST", "CANC"):
            self.assertFalse(fh.eligibility(entry, {**fx, "fixture_status": status}, NOW)[0])
        self.assertFalse(fh.eligibility(entry, {**fx, "kickoff_utc": NOW}, NOW)[0])

    def test_grading_and_unsettled_states(self):
        e = {"event_key": "match:over:1.5", "recorded_at": NOW}
        fx = {"kickoff_utc": NOW + timedelta(hours=1), "fixture_status": "FT",
              "home_team_fulltime_goal": 1, "away_team_fulltime_goal": 1}
        self.assertEqual(fh.grade(e, fx)[0], "hit")
        self.assertEqual(fh.grade(e, {**fx, "away_team_fulltime_goal": 0})[0], "miss")
        for status in ("PST", "SUSP", "NS", "1H"):
            self.assertEqual(fh.grade(e, {**fx, "fixture_status": status})[0], "pending")
        self.assertEqual(fh.grade(e, {**fx, "fixture_status": "CANC"})[0], "void")
        for status in ("ABD", "AWD", "WO", "AET", "PEN", None):
            self.assertEqual(fh.grade(e, {**fx, "fixture_status": status})[0], "unavailable")
        self.assertEqual(fh.grade(e, {**fx, "home_team_fulltime_goal": None})[0], "unavailable")
        self.assertEqual(fh.grade(e, {**fx, "kickoff_utc": NOW})[0], "excluded")
        for key, hit in (("match:bounds:1:4", True), ("home:under:1.5", True),
                         ("away:over:1.5", False), ("btts:no", False)):
            self.assertEqual(fh.grade({**e, "event_key": key}, fx)[0], "hit" if hit else "miss")

    def test_capture_happens_before_feed_publication(self):
        import tempfile
        from pathlib import Path
        from engine.eval.consensus_slips import save_slips
        result = dict(pool=[], near_threshold=[], slips=[], fixture_analysis=[],
                      bounds_1_4_shortlist=[], over_1_5_shortlist=[], elo_gap_shortlist=[])
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "consensus_slips_2026-10-02.json"
            with patch.object(fh, "init_schema"), patch.object(fh, "capture", side_effect=RuntimeError("archive failed")):
                with self.assertRaisesRegex(RuntimeError, "archive failed"):
                    save_slips(result, "2026-10-02", Path(td), record_history=True)
                self.assertFalse(target.exists())
            from engine.eval import match_store
            with patch.object(fh, "init_schema"), patch.object(fh, "capture", return_value=1), \
                    patch.object(match_store, "write_match_analysis", side_effect=RuntimeError("store failed")):
                with self.assertRaisesRegex(RuntimeError, "store failed"):
                    save_slips(result, "2026-10-02", Path(td), record_history=True)
                self.assertFalse(target.exists())
            with patch.object(fh, "init_schema"), patch.object(fh, "capture", return_value=1) as capture, \
                    patch.object(match_store, "write_match_analysis", return_value=0) as store:
                save_slips(result, "2026-10-02", Path(td), record_history=True)
                capture.assert_called_once()
                store.assert_called_once()
                self.assertEqual(store.call_args[0][0], "2026-10-02")
                self.assertTrue(target.exists())


@unittest.skipUnless(os.getenv("FORECAST_TEST_DSN"), "requires isolated FORECAST_TEST_DSN")
class ForecastPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg2
        cls.conn = psycopg2.connect(os.environ["FORECAST_TEST_DSN"])
        if not cls.conn.get_dsn_parameters()["dbname"].startswith("forecast_test"):
            cls.conn.close()
            raise RuntimeError("Integration tests require a forecast_test* database")
        with cls.conn, cls.conn.cursor() as cur:
            cur.execute("SELECT to_regnamespace('forecast'), to_regclass('raw.fact_fixtures')")
            if any(cur.fetchone()):
                raise RuntimeError("Integration tests require a fresh empty database")
            cur.execute("""CREATE SCHEMA raw;
                CREATE TABLE raw.fact_fixtures (
                  fixture_id bigint PRIMARY KEY, kickoff_utc timestamptz,
                  fixture_status text, home_team_fulltime_goal int, away_team_fulltime_goal int);
                INSERT INTO raw.fact_fixtures VALUES (1, clock_timestamp() + interval '1 day', 'NS', NULL, NULL);""")
        fh.init_schema(cls.conn)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_capture_retry_latest_choice_corrections_and_immutability(self):
        p = payload(datetime.now(timezone.utc) + timedelta(days=1))
        first = fh.capture(p, self.conn)
        self.assertEqual(fh.capture(p, self.conn), first)
        later = copy.deepcopy(p)
        later["fixture_analysis"][0]["dixon_coles"][0]["probability"] = .95
        latest = fh.capture(later, self.conn)
        self.assertNotEqual(latest, first)
        self.assertEqual(fh.capture(p, self.conn), first)  # Retry cannot promote an older capture.
        fh.init_schema(self.conn)  # Repeated schema setup retains the selection.
        self.assertEqual(fh.report(self.conn)["records"][0]["probability"], .95)
        self.assertEqual(fh.report(self.conn)["policy"], "latest_pre_kickoff_dc_goals_v1")
        self.assertEqual(fh.settle(self.conn), 1)
        self.assertEqual(fh.settle(self.conn), 0)
        with self.conn, self.conn.cursor() as cur:
            cur.execute("UPDATE raw.fact_fixtures SET fixture_status='FT', kickoff_utc=clock_timestamp(), home_team_fulltime_goal=1, away_team_fulltime_goal=1")
        self.assertEqual(fh.settle(self.conn), 1)
        self.assertEqual(fh.report(self.conn)["counts"]["hit"], 1)
        with self.conn, self.conn.cursor() as cur:
            cur.execute("UPDATE raw.fact_fixtures SET away_team_fulltime_goal=0")
        self.assertEqual(fh.settle(self.conn), 1)
        self.assertEqual(fh.report(self.conn)["counts"]["miss"], 1)
        with self.conn, self.conn.cursor() as cur:
            cur.execute("UPDATE raw.fact_fixtures SET away_team_fulltime_goal=1")
        self.assertEqual(fh.settle(self.conn), 1)  # Reversal is not incorrectly deduplicated.
        self.assertEqual([r["status"] for r in fh.report(self.conn)["records"][0]["outcome_history"]],
                         ["pending", "hit", "miss", "hit"])
        late = copy.deepcopy(p)
        late["run_ds"] = "2020-01-01"  # Logical backdate cannot change real capture time.
        fh.capture(late, self.conn)
        with self.conn.cursor() as cur:
            cur.execute("SELECT eligible FROM forecast.entries ORDER BY id DESC LIMIT 1")
            self.assertFalse(cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM forecast.settlements")
            self.assertEqual(cur.fetchone()[0], 4)
        import psycopg2
        for table in ("snapshots", "entries", "settlements"):
            for statement in (f"DELETE FROM forecast.{table}", f"UPDATE forecast.{table} SET id=id", f"TRUNCATE forecast.{table} CASCADE"):
                with self.assertRaises(psycopg2.Error):
                    with self.conn, self.conn.cursor() as cur:
                        cur.execute(statement)
        self.assertEqual(fh.report(self.conn)["records"][0]["probability"], .95)
        with self.conn.cursor() as cur:
            cur.execute("SELECT probability FROM forecast.entries WHERE snapshot_id=%s", (first,))
            self.assertEqual(cur.fetchone()[0], .8)

    def test_no_reopening_after_captured_kickoff_and_policy_isolation(self):
        with self.conn, self.conn.cursor() as cur:
            cur.execute("""INSERT INTO raw.fact_fixtures VALUES
                           (2, clock_timestamp() + interval '1 day', 'NS', NULL, NULL)""")
        p = payload(datetime.now(timezone.utc) + timedelta(days=1))
        p["fixture_analysis"][0]["fixture_id"] = 2
        with patch.object(fh, "POLICY", "first_pre_kickoff_dc_goals_v1"):
            legacy = fh.capture(p, self.conn)
        # Existing snapshots are never relabelled or backfilled into the new policy.
        self.assertEqual(fh.capture(p, self.conn), legacy)
        self.assertFalse(any(r["fixture_id"] == 2 for r in fh.report(self.conn)["records"]))
        p["fixture_analysis"][0]["dixon_coles"][0]["probability"] = .7
        fh.capture(p, self.conn)
        # Seed an elapsed eligible capture in the isolated DB to avoid wall-clock sleeps.
        with self.conn, self.conn.cursor() as cur:
            cur.execute("""INSERT INTO forecast.snapshots
                           (content_hash,run_ds,recorded_at,payload,provenance)
                           VALUES ('elapsed-test',CURRENT_DATE,clock_timestamp()-interval '2 hours','{}','{}')
                           RETURNING id""")
            sid = cur.fetchone()[0]
            cur.execute("""INSERT INTO forecast.entries
                           (snapshot_id,fixture_id,event_key,probability,kickoff,eligible,
                            eligibility_reason,policy,detail)
                           VALUES (%s,2,'match:over:1.5',.6,clock_timestamp()-interval '1 hour',
                                   true,'pre_kickoff_capture',%s,'{}')""", (sid, fh.POLICY))
        p["fixture_analysis"][0]["dixon_coles"][0]["probability"] = .99
        fh.capture(p, self.conn)
        with self.conn.cursor() as cur:
            cur.execute("SELECT eligible,eligibility_reason FROM forecast.entries ORDER BY id DESC LIMIT 1")
            self.assertEqual(cur.fetchone(), (False, "previously_captured_kickoff_reached"))
        selected = next(r for r in fh.report(self.conn)["records"] if r["fixture_id"] == 2)
        self.assertEqual(selected["probability"], .7)

    def test_selection_is_per_event_with_deterministic_timestamp_ties(self):
        with self.conn, self.conn.cursor() as cur:
            for index, predictions in enumerate((
                    (("match:over:1.5", .6), ("btts:yes", .5)),
                    (("match:over:1.5", .9),))):
                cur.execute("""INSERT INTO forecast.snapshots
                               (content_hash,run_ds,recorded_at,payload,provenance)
                               VALUES (%s,CURRENT_DATE,%s,'{}','{}') RETURNING id""",
                            (f"tie-{index}", NOW))
                sid = cur.fetchone()[0]
                for key, probability in predictions:
                    cur.execute("""INSERT INTO forecast.entries
                                   (snapshot_id,fixture_id,event_key,probability,kickoff,eligible,
                                    eligibility_reason,policy,detail)
                                   VALUES (%s,3,%s,%s,%s,true,'pre_kickoff_capture',%s,'{}')""",
                                (sid, key, probability, NOW + timedelta(hours=1), fh.POLICY))
        records = {r["event_key"]: r["probability"] for r in fh.report(self.conn)["records"]
                   if r["fixture_id"] == 3}
        self.assertEqual(records, {"match:over:1.5": .9, "btts:yes": .5})


if __name__ == "__main__":
    unittest.main()
