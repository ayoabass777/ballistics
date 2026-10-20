"""New-fixture discovery: insert what the API has and raw.raw_fixtures doesn't."""
import os
import unittest
from unittest.mock import MagicMock, patch

from etl.src import new_fixtures as nf
from etl.src.s3_landing import RAW_COLUMNS


def fx(fid, league=244, season=2026, home=10, away=11, kickoff="2026-10-25T15:00:00+00:00",
       status="NS", round_="Championship Group - 23"):
    return {"api_fixture_id": fid, "api_league_id": league, "season": season, "kickoff_utc": kickoff,
            "fixture_status": status, "home_team_id": home, "home_team_name": f"H{fid}",
            "away_team_id": away, "away_team_name": f"A{fid}", "round": round_}


class Patched(unittest.TestCase):
    """fetch_fixtures returns the records per league; extract passes them through."""

    def setUp(self):
        self.api = {}
        for name in ("park_fixture_rows", "set_status"):
            stub = patch.object(nf, name)
            setattr(self, name, stub.start())
            self.addCleanup(stub.stop)
        for target, fn in (
            ("etl.src.new_fixtures.fetch_fixtures", self.fetch),
            ("etl.src.new_fixtures.extract_fixtures_field", lambda payload: payload),
        ):
            p = patch(target, fn)
            p.start()
            self.addCleanup(p.stop)

    def fetch(self, endpoint, params, **kwargs):
        result = self.api[(params["league"], params["season"])]
        if isinstance(result, Exception):
            raise result
        return result


class InsertNewFixturesTests(Patched):
    def run_with(self, leagues, have=()):
        conn = MagicMock()
        inserted = []
        with patch.object(nf, "current_league_seasons", return_value=leagues), \
                patch.object(nf, "missing_fixtures",
                             lambda conn, records: [r for r in records if r["api_fixture_id"] not in have]), \
                patch.object(nf, "insert_fixtures",
                             lambda conn, records: inserted.extend(records) or len(records)):
            summary = nf.insert_new_fixtures(conn)
        return summary, inserted, conn

    def test_inserts_only_missing_and_valid_rows(self):
        self.api[(244, 2026)] = [fx(1), fx(2), fx(3, home=None)]
        summary, inserted, conn = self.run_with([(244, 2026)], have={1})
        self.assertEqual([r["api_fixture_id"] for r in inserted], [2])
        self.assertEqual((summary["new"], summary["inserted"], summary["skipped_invalid"]), (2, 1, 1))
        self.assertEqual(summary["inserted_by_league"], {"244/2026": 1})
        conn.commit.assert_called_once()

    def test_one_failed_league_does_not_stop_the_others(self):
        self.api[(98, 2027)] = OSError("timeout")
        self.api[(244, 2026)] = [fx(5)]
        summary, inserted, _ = self.run_with([(98, 2027), (244, 2026)])
        self.assertEqual(summary["failed"], ["98/2027"])
        self.assertEqual(summary["inserted"], 1)

    def test_invalid_records_without_id_and_existing_ids_are_parked(self):
        self.api[(244, 2026)] = [fx(None), fx(1, home=None), fx(2)]
        summary, inserted, _ = self.run_with([(244, 2026)], have={1})
        self.assertEqual([r["api_fixture_id"] for r in inserted], [2])
        self.assertEqual(summary["skipped_invalid"], 2)
        bad = self.park_fixture_rows.call_args.args[0]
        self.assertEqual([r["api_fixture_id"] for r in bad], [None, 1])
        self.set_status.assert_called_once()

    def test_every_league_failing_raises(self):
        self.api[(98, 2027)] = OSError("plan lapsed")
        self.api[(244, 2026)] = OSError("plan lapsed")
        with self.assertRaises(RuntimeError):
            self.run_with([(98, 2027), (244, 2026)])

    def test_api_empty_for_every_league_raises(self):
        self.api[(98, 2027)] = []                                   # what a missing key looks like
        self.api[(244, 2026)] = []
        with self.assertRaises(RuntimeError):
            self.run_with([(98, 2027), (244, 2026)])

    def test_insert_error_rolls_back_that_league_only(self):
        self.api[(98, 2027)] = [fx(7, league=98, season=2027)]
        self.api[(244, 2026)] = [fx(8)]
        conn = MagicMock()
        calls = []

        def insert(conn, records):
            calls.append(records[0]["api_fixture_id"])
            if records[0]["api_fixture_id"] == 7:
                raise ValueError("bad row")
            return len(records)

        with patch.object(nf, "current_league_seasons", return_value=[(98, 2027), (244, 2026)]), \
                patch.object(nf, "missing_fixtures", lambda conn, records: records), \
                patch.object(nf, "insert_fixtures", insert):
            summary = nf.insert_new_fixtures(conn)
        self.assertEqual(calls, [7, 8])
        self.assertEqual((summary["failed"], summary["inserted"]), (["98/2027"], 1))
        conn.rollback.assert_called_once()
        conn.commit.assert_called_once()


class FetchFailureTests(unittest.TestCase):
    def test_transport_failure_is_reported_and_next_league_continues(self):
        from requests.exceptions import Timeout
        from etl.src import extract_fixtures as ef

        response = MagicMock(status_code=200, headers={})
        response.json.return_value = {"response": [fx(5)]}
        with patch.object(ef.requests, "get", side_effect=[Timeout("offline")] * 5 + [response]), \
                patch.object(ef.time, "sleep"), \
                patch.object(nf, "current_league_seasons", return_value=[(98, 2027), (244, 2026)]), \
                patch.object(nf, "extract_fixtures_field", side_effect=lambda payload: payload), \
                patch.object(nf, "missing_fixtures", side_effect=lambda conn, records: records), \
                patch.object(nf, "insert_fixtures", return_value=1):
            summary = nf.insert_new_fixtures(MagicMock())
        self.assertEqual(summary["failed"], ["98/2027"])
        self.assertEqual(summary["inserted"], 1)

    def test_strict_fetch_distinguishes_empty_success_from_invalid_json(self):
        from etl.src import extract_fixtures as ef

        response = MagicMock(status_code=200, headers={})
        response.json.return_value = {"response": []}
        with patch.object(ef.requests, "get", return_value=response):
            self.assertEqual(ef.fetch_fixtures("fixtures", {}, raise_on_failure=True), [])
            response.json.side_effect = ValueError("invalid JSON")
            with self.assertRaises(ef.ApiResponseError):
                ef.fetch_fixtures("fixtures", {}, raise_on_failure=True)


@unittest.skipUnless(os.getenv("FLOW_TEST_DSN"), "requires FLOW_TEST_DSN (throwaway database)")
class PostgresNewFixturesTests(Patched):
    def setUp(self):
        super().setUp()
        import psycopg2
        self.conn = psycopg2.connect(os.environ["FLOW_TEST_DSN"])
        cols = ", ".join(f"{c} TEXT" for c in RAW_COLUMNS if c not in
                         ("api_fixture_id", "api_league_id", "season", "kickoff_utc", "fixture_status",
                          "home_team_id", "away_team_id"))
        with self.conn.cursor() as cur:
            cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS raw; CREATE SCHEMA IF NOT EXISTS raw_int;
                DROP TABLE IF EXISTS raw.raw_fixtures, raw_int.int_league_context;
                CREATE TABLE raw_int.int_league_context (api_league_id INT, season INT, is_current BOOLEAN);
                INSERT INTO raw_int.int_league_context VALUES (244, 2026, true), (244, 2025, false);
                CREATE TABLE raw.raw_fixtures (fixture_id BIGSERIAL PRIMARY KEY,
                  api_fixture_id BIGINT NOT NULL UNIQUE, api_league_id INT NOT NULL, season INT NOT NULL,
                  kickoff_utc TIMESTAMPTZ NOT NULL, fixture_status TEXT NOT NULL,
                  home_team_id INT, away_team_id INT, {cols});
                INSERT INTO raw.raw_fixtures (api_fixture_id, api_league_id, season, kickoff_utc,
                  fixture_status, home_team_id, away_team_id, home_team_name)
                VALUES (1, 244, 2026, '2026-10-18', 'FT', 10, 11, 'kept as is');""")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def rows(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT fixture_id, api_fixture_id, home_team_name, round "
                        "FROM raw.raw_fixtures ORDER BY api_fixture_id")
            return cur.fetchall()

    def test_inserts_new_rows_and_leaves_existing_ones(self):
        before = self.rows()
        self.api[(244, 2026)] = [fx(1), fx(2), fx(3)]
        summary = nf.insert_new_fixtures(self.conn)
        after = self.rows()
        self.assertEqual(after[0], before[0])                       # same fixture_id, untouched
        self.assertEqual([r[1] for r in after], [1, 2, 3])
        self.assertEqual(after[1][3], "Championship Group - 23")
        self.assertEqual(summary["inserted"], 2)

    def test_second_run_inserts_nothing(self):
        self.api[(244, 2026)] = [fx(1), fx(2)]
        nf.insert_new_fixtures(self.conn)
        summary = nf.insert_new_fixtures(self.conn)
        self.assertEqual((summary["new"], summary["inserted"]), (0, 0))
        self.assertEqual(len(self.rows()), 2)

    def test_only_current_seasons_are_pulled(self):
        self.api[(244, 2026)] = [fx(1)]
        summary = nf.insert_new_fixtures(self.conn)                 # (244, 2025) would KeyError
        self.assertEqual(summary["league_seasons"], 1)


if __name__ == "__main__":
    unittest.main()
