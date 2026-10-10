"""HTTP/SQL integration on a fresh, opt-in forecast_test* database only."""
from datetime import datetime, timedelta, timezone
import os
import unittest
from unittest.mock import patch

import psycopg2
from psycopg2.extras import Json
from fastapi.testclient import TestClient

from engine.api.app import app
from engine.api import forecast_records as api
from engine.eval import forecast_history as ledger


class ForecastHTTPValidationTests(unittest.TestCase):
    def test_invalid_queries_do_not_connect(self):
        with TestClient(app) as client, patch.object(api,"connect") as connect:
            for query in ("limit=101","limit=0","offset=-1","outcome=winner", "date_from=bad",
                          "offset=100001", "team=" + "a"*101):
                self.assertEqual(client.get("/api/forecast-record?"+query).status_code,422)
            connect.assert_not_called()

    def test_unavailable_is_not_an_empty_success_or_secret_leak(self):
        with TestClient(app) as client, patch.object(api,"connect",side_effect=RuntimeError("secret-dsn")):
            result = client.get("/api/forecast-record")
            self.assertEqual(result.status_code,503)
            self.assertNotIn("secret",result.text)


@unittest.skipUnless(os.getenv("FORECAST_API_TEST_DSN"),"requires isolated FORECAST_API_TEST_DSN")
class ForecastAPIIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dsn = os.environ["FORECAST_API_TEST_DSN"]
        cls.writer = psycopg2.connect(cls.dsn)
        if not cls.writer.get_dsn_parameters()["dbname"].startswith("forecast_test"):
            cls.writer.close()
            raise RuntimeError("Test database must start with forecast_test")
        with cls.writer, cls.writer.cursor() as cur:
            cur.execute("SELECT to_regnamespace('forecast')")
            if cur.fetchone()[0] is not None:
                raise RuntimeError("Use a fresh empty database")
        ledger.init_schema(cls.writer)
        cls.now = datetime.now(timezone.utc)
        cls.entries = {}
        with cls.writer, cls.writer.cursor() as cur:
            for fid, home, away, league, country, future in (
                    (1,"Brooklyn","Detroit","USL","USA",False),
                    (2,"Home","Brooklyn","Serie B","Brazil",False),
                    (3,"Future","Visitor","USL","USA",True)):
                kickoff = cls.now + timedelta(days=1 if future else -1)
                captured = kickoff - timedelta(days=2)
                cur.execute("""INSERT INTO forecast.snapshots
                    (content_hash,run_ds,recorded_at,payload,provenance)
                    VALUES (%s,CURRENT_DATE,%s,'{}','{}') RETURNING id""",(str(fid),captured))
                sid = cur.fetchone()[0]
                for market, status in (("match:over:1.5","hit"),("btts:yes","pending")):
                    detail = {"fixture":dict(home=home,away=away,league=league,country=country),
                              "forecast":{"market":market},
                              "fixture_at_capture":{"kickoff_utc":kickoff.isoformat()}}
                    cur.execute("""INSERT INTO forecast.entries
                        (snapshot_id,fixture_id,event_key,probability,kickoff,eligible,eligibility_reason,policy,detail)
                        VALUES (%s,%s,%s,.8,%s,true,'pre_kickoff_capture',%s,%s) RETURNING id""",
                                (sid,fid,market,kickoff,ledger.POLICY,Json(detail)))
                    eid = cur.fetchone()[0]
                    cls.entries[(fid,market)] = eid
                    if not future:
                        cur.execute("""INSERT INTO forecast.settlements
                            (entry_id,observed_at,grader,status,reason,evidence,evidence_hash)
                            VALUES (%s,%s,'test',%s,'test','{}',%s)""",
                                    (eid,cls.now-timedelta(hours=1),status,f"{eid}-initial"))
        cls.client = TestClient(app)
        cls.connector = patch.object(api,"connect",side_effect=lambda:psycopg2.connect(cls.dsn))
        cls.connector.start()

    @classmethod
    def tearDownClass(cls):
        cls.connector.stop()
        cls.client.close()
        cls.writer.close()

    def get(self, **params):
        result = self.client.get("/api/forecast-record",params=params)
        self.assertEqual(result.status_code,200,result.text)
        self.assertEqual(result.headers["cache-control"],"no-store")
        return result.json()

    def test_grouping_totals_and_pagination(self):
        page = self.get(limit=1)
        self.assertEqual(page["totals"],dict(fixtures=3,forecasts=6))
        self.assertEqual(len(page["fixtures"]),1)
        self.assertEqual(len(page["fixtures"][0]["forecasts"]),2)
        self.assertEqual(page["fixtures"][0]["selection_state"],"provisional")
        self.assertEqual(page["fixtures"][0]["forecasts"][0]["result_observation_state"],"unobserved")
        ids = [page["fixtures"][0]["fixture_id"]]
        for offset in (1,2):
            later = self.get(limit=1,offset=offset,as_of=page["as_of"])
            ids.append(later["fixtures"][0]["fixture_id"])
        self.assertEqual(len(set(ids)),3)
        self.assertIsNone(later["pagination"]["next_offset"])
        self.assertEqual(self.get(offset=99)["fixtures"],[])

    def test_league_directory_is_from_captured_ledger(self):
        response = self.client.get('/api/forecast-record/filters')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json()['leagues'],[
            dict(country='Brazil',league='Serie B'),dict(country='USA',league='USL')])

    def test_filters_mixed_outcomes_and_empty(self):
        data = self.get(team="brookLYN",league="USL",country="USA",outcome="settled")
        self.assertEqual(data["totals"],dict(fixtures=1,forecasts=1))
        self.assertEqual(data["cohort"]["forecasts"],2)
        self.assertEqual(data["cohort"]["status_counts"]["pending"],1)
        self.assertEqual(data["fixtures"][0]["selection_state"],"final")
        self.assertEqual(self.get(team="Brooklyn")["totals"]["fixtures"],2)
        self.assertEqual(self.get(team="%_")["totals"]["fixtures"],0)
        self.assertEqual(self.get(team="' OR true --")["totals"]["fixtures"],0)
        self.assertEqual(self.get(team="Brooklyn",country="France")["totals"]["forecasts"],0)
        day = (self.now + timedelta(days=1)).date().isoformat()
        self.assertEqual(self.get(date_from=day,date_to=day)["totals"]["fixtures"],1)

    def test_invalid_ranges_times_and_history_ids(self):
        for query in (dict(date_from="2026-10-03",date_to="2026-10-02"),
                      dict(as_of="2026-01-01T00:00:00"),
                      dict(as_of=(self.now+timedelta(days=10)).isoformat())):
            self.assertEqual(self.client.get("/api/forecast-record",params=query).status_code,422)
        self.assertEqual(self.client.get("/api/forecast-record/entries/9999/history").status_code,404)

    def test_readonly_transaction(self):
        with api.read_cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            self.assertEqual(cur.fetchone()["transaction_read_only"],"on")
            with self.assertRaises(psycopg2.errors.ReadOnlySqlTransaction):
                cur.execute("INSERT INTO forecast.snapshots DEFAULT VALUES")

    def test_z_corrections_and_frozen_browsing(self):
        before = self.get(team="Brooklyn",league="USL")
        eid = self.entries[(1,"match:over:1.5")]
        with self.writer, self.writer.cursor() as cur:
            cur.execute("""INSERT INTO forecast.settlements
                (entry_id,grader,status,reason,evidence,evidence_hash)
                VALUES (%s,'test','miss','score_correction','{}','correction')""",(eid,))
        frozen = self.get(team="Brooklyn",league="USL",as_of=before["as_of"])
        self.assertEqual(frozen["fixtures"],before["fixtures"])
        fresh = self.get(team="Brooklyn",league="USL")
        self.assertEqual(fresh["cohort"]["status_counts"]["miss"],1)
        url = f"/api/forecast-record/entries/{eid}/history"
        first = self.client.get(url,params=dict(limit=1,as_of=fresh["as_of"])).json()
        second = self.client.get(url,params=dict(limit=1,as_of=fresh["as_of"],after_id=first["next_after_id"])).json()
        self.assertEqual(first["observations"][0]["status"],"hit")
        self.assertEqual(second["observations"][0]["status"],"miss")
        self.assertEqual(second["forecast"]["probability"],.8)
        self.assertIsNone(second["next_after_id"])

    def test_zz_capture_settle_http_end_to_end(self):
        import copy
        with self.writer, self.writer.cursor() as cur:
            cur.execute("""CREATE SCHEMA raw;
                CREATE TABLE raw.raw_fixtures (fixture_id bigint PRIMARY KEY,kickoff_utc timestamptz,
                  fixture_status text,home_team_fulltime_goal int,away_team_fulltime_goal int);
                INSERT INTO raw.raw_fixtures VALUES (9,clock_timestamp()+interval '1 day','NS',NULL,NULL)""")
        p = dict(run_ds=self.now.date().isoformat(),fixture_analysis=[dict(fixture_id=9,
                 home="Pipeline",away="Opponent",league="Test",country="Test",
                 kickoff=(datetime.now(timezone.utc)+timedelta(days=1)).isoformat(),
                 dixon_coles=[dict(market="Match Over 1.5",probability=.6)])])
        ledger.capture(p,self.writer)
        p = copy.deepcopy(p)
        p["fixture_analysis"][0]["dixon_coles"][0]["probability"] = .9
        ledger.capture(p,self.writer)
        before = self.get(team="Pipeline")
        self.assertEqual(before["fixtures"][0]["forecasts"][0]["probability"],.9)
        p["fixture_analysis"][0]["dixon_coles"][0]["probability"] = .85
        ledger.capture(p,self.writer)
        frozen = self.get(team="Pipeline",as_of=before["as_of"])
        self.assertEqual(frozen["fixtures"],before["fixtures"])
        with self.writer, self.writer.cursor() as cur:
            cur.execute("""UPDATE raw.raw_fixtures SET fixture_status='FT',
                kickoff_utc=clock_timestamp(),home_team_fulltime_goal=2,away_team_fulltime_goal=1""")
        ledger.settle(self.writer)
        final = self.get(team="Pipeline")["fixtures"][0]["forecasts"][0]
        self.assertEqual(final["probability"],.85)
        self.assertEqual(final["status"],"hit")


if __name__ == "__main__":
    unittest.main()
