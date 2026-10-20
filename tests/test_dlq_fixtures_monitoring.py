"""D148: fixture row DLQ, @monitored, and the API data decorators."""
import os
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from etl.src import dlq
from etl.src import fixtures_dlq as fd
from etl.src.data_detector import monitoring
from engine.api import data_quality as dq

from tests.test_dlq import FakeS3

LEAGUE = {"api_league_id": 262, "season": 2026}
KEY = "dlq/fixtures/rows/262/2026/open.json"


def fx(fid, home=10, away=11, kickoff="2026-10-20T01:00:00+00:00", status="NS"):
    return {"api_fixture_id": fid, "api_league_id": 262, "season": 2026, "kickoff_utc": kickoff,
            "fixture_status": status, "home_team_id": home, "home_team_name": f"H{fid}",
            "away_team_id": away, "away_team_name": f"A{fid}"}


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=None):
        self.log.append(("execute", " ".join(sql.split()), params))

    def copy_expert(self, sql, buf):
        self.log.append(("copy", buf.getvalue().count("\n")))

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.log = []

    def cursor(self):
        return FakeCursor(self.log)


class FixtureRowProblemTests(unittest.TestCase):
    def test_reasons(self):
        self.assertIsNone(fd.fixture_row_problem(fx(1)))
        self.assertEqual(fd.fixture_row_problem(fx(None)), "null_fixture_id")
        self.assertEqual(fd.fixture_row_problem(fx(1, kickoff=None)), "null_kickoff")
        self.assertEqual(fd.fixture_row_problem(fx(1, status="")), "null_status")
        self.assertEqual(fd.fixture_row_problem(fx(1, away=None)), "null_team_id")


class FixtureLoadTests(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.s3.install(self)
        self.statuses, self.records = [], []
        for target, fn in (
            ("etl.src.fixtures_dlq.set_status",
             lambda league, bad, parked, conn=None: self.statuses.append((len(bad), parked))),
            ("etl.src.data_detector.monitoring.record",
             lambda *a, **k: self.records.append((a, k))),
        ):
            p = patch(target, fn)
            p.start()
            self.addCleanup(p.stop)

    def load(self, rows, source="bootstrap"):
        conn = FakeConn()
        result = fd.load_league_fixtures(rows, league=LEAGUE, ds="2026-10-19", conn=conn, source=source)
        return result, conn.log

    def test_extraction_rejects_are_parked_and_disable_pruning(self):
        from etl.src.extract_fixtures import extract_fixtures_field
        payload = [{"fixture": {"id": 2}}, {"fixture": ["invalid"]}, None]
        rejected = extract_fixtures_field(payload)
        self.assertEqual(len(rejected), 3)
        self.assertEqual([r["_raw_payload"] for r in rejected], payload)
        result, log = self.load([fx(1), *rejected])
        self.assertEqual((result.loaded, len(result.parked)), (1, 3))
        self.assertEqual(self.s3.objects[KEY]["rows"][0]["_raw_payload"], payload[0])
        self.assertFalse(any(e[1].startswith("DELETE FROM raw.raw_fixtures")
                             for e in log if e[0] == "execute"))

    def test_bad_row_parked_good_rows_replace_only_themselves(self):
        result, log = self.load([fx(1), fx(2, home=None), fx(3)])
        self.assertEqual((result.loaded, len(result.parked)), (2, 1))
        self.assertIn(("copy", 2), log)
        self.assertTrue(any(e[1].startswith("UPDATE raw.raw_fixtures") for e in log if e[0] == "execute"))
        self.assertFalse(any(e[1].startswith("DELETE FROM raw.raw_fixtures") for e in log
                             if e[0] == "execute"))                 # fixture 2 keeps its last good row
        entry = self.s3.objects[KEY]
        self.assertEqual((entry["reasons"], entry["fixtures"]), (["null_team_id"], ["H2 v A2 (2)"]))
        self.assertTrue(result.new_entry)
        self.assertEqual(self.statuses[-1][0], 1)

    def test_clean_load_replaces_league_and_resolves_after_commit(self):
        self.load([fx(1), fx(2, home=None)])
        conn = FakeConn()
        result = fd.load_league_fixtures([fx(1), fx(2)], league=LEAGUE, ds="2026-10-19",
                                         conn=conn, source="replay")
        self.assertTrue(any(e[1].startswith("DELETE FROM raw.raw_fixtures") and "NOT EXISTS" in e[1]
                            for e in conn.log if e[0] == "execute"))
        self.assertTrue(result.resolved)
        self.assertEqual(self.statuses[-1], (0, None))
        self.assertIn(KEY, self.s3.objects)                         # not before the caller commits
        dlq.run_after_commit(conn)
        self.assertNotIn(KEY, self.s3.objects)
        self.assertEqual(len(self.s3.list_keys("dlq/resolved/")), 1)

    def test_rolled_back_clean_load_keeps_the_entry(self):
        self.load([fx(1), fx(2, home=None)])
        conn = FakeConn()
        fd.load_league_fixtures([fx(1), fx(2)], league=LEAGUE, ds="2026-10-19", conn=conn)
        dlq.discard_after_commit(conn)                              # caller's commit failed
        dlq.run_after_commit(conn)
        self.assertIn(KEY, self.s3.objects)

    def test_second_sighting_is_not_new(self):
        self.load([fx(1, kickoff=None)])
        result, _ = self.load([fx(1, kickoff=None)])
        self.assertFalse(result.new_entry)
        self.assertEqual(self.s3.objects[KEY]["attempts"], 0)       # nightly/bootstrap don't count

    def test_replay_still_bad_counts_and_load_is_monitored(self):
        self.load([fx(1, kickoff=None)])                              # entry opened at 0
        self.load([fx(1, kickoff=None)], source="replay")
        self.assertEqual(self.s3.objects[KEY]["attempts"], 1)
        dataset, table, league, result = self.records[-1][0]
        self.assertEqual((dataset, table, self.records[-1][1]["source"]), ("fixtures", "raw.raw_fixtures", "replay"))


    def test_entry_first_seen_by_replay_starts_at_zero(self):
        parked = fd.park_fixture_rows([{**fx(1, kickoff=None), dlq.DLQ_REASON: "null_kickoff"}],
                                      league=LEAGUE, ds="2026-10-20", source="replay")
        self.assertEqual(parked[1]["attempts"], 0)


class FixtureReplayTests(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.s3.install(self)
        self.s3.objects[KEY] = {"kind": "fixture_rows", **LEAGUE, **dlq.new_state(), "rows": [fx(2, home=None)]}
        self.s3.objects[KEY]["next_retry_at"] = None
        self.failed = []
        p = patch("etl.src.fixtures_dlq.set_status", lambda *a, **k: None)
        p.start()
        self.addCleanup(p.stop)

    def run_replay(self, fixtures, bad):
        conn = MagicMock()
        with patch("etl.src.extract_fixtures.fetch_fixtures", return_value={"response": fixtures}), \
                patch("etl.src.extract_fixtures.extract_fixtures_field", return_value=fixtures), \
                patch("etl.src.s3_landing.write_fixtures_to_s3", return_value="full_load/262/2026/d/fixtures.json"), \
                patch("etl.src.s3_landing.load_fixtures_from_s3",
                      return_value=dlq.LoadResult(loaded=1, parked=bad)) as load, \
                patch("etl.src.extract_metadata.get_db_connection", return_value=conn):
            summary = fd.replay_fixture_rows_dlq(ds="2026-10-20")
        return summary, load, conn

    def test_due_entry_reloads_with_replay_source(self):
        summary, load, conn = self.run_replay([fx(1)], bad=[])
        self.assertEqual(summary["resolved"], 1)
        self.assertEqual(load.call_args.kwargs, {"source": "replay", "detailed": True})
        conn.commit.assert_called_once()

    def test_empty_response_counts_an_attempt(self):
        summary, load, _ = self.run_replay([], bad=[])
        load.assert_not_called()
        self.assertEqual((summary["still_bad"], self.s3.objects[KEY]["attempts"]), (1, 1))

    def dead(self, hours_ago):
        last = dlq._iso(dlq.now_utc() - timedelta(hours=hours_ago))
        self.s3.objects[KEY].update(status=dlq.DEAD, attempts=24, last_attempt_at=last, next_retry_at=None)

    def test_dead_entry_waits_a_day_for_its_recheck(self):
        self.dead(hours_ago=2)
        summary, load, _ = self.run_replay([fx(1)], bad=[])
        load.assert_not_called()
        self.assertEqual((summary["skipped"], summary["dead_rechecked"]), (1, 0))

    def test_dead_entry_recheck_clean_resolves(self):
        self.dead(hours_ago=25)
        summary, load, _ = self.run_replay([fx(1)], bad=[])
        load.assert_called_once()
        self.assertEqual((summary["dead_rechecked"], summary["resolved"]), (1, 1))

    def test_dead_entry_recheck_still_failing_stays_dead_without_alert(self):
        self.dead(hours_ago=25)
        with patch.object(dlq, "alert_dead") as alert:
            summary, _, _ = self.run_replay([], bad=[])
        entry = self.s3.objects[KEY]
        self.assertEqual((entry["status"], entry["attempts"], entry["next_retry_at"]), (dlq.DEAD, 25, None))
        alert.assert_not_called()
        self.assertFalse(dlq.dead_recheck_due(entry))      # next recheck tomorrow


class MonitoredTests(unittest.TestCase):
    def setUp(self):
        self.calls = {"movement": [], "event": [], "resolve": [], "alert": []}
        for target, name in (
            ("etl.src.data_detector.data_movements.record_data_movement", "movement"),
            ("etl.src.data_detector.quality_events.record_data_quality_event", "event"),
            ("etl.src.data_detector.quality_events.resolve_data_quality_events", "resolve"),
            ("etl.src.data_detector.notify.send_alert", "alert"),
        ):
            p = patch(target, lambda *a, _n=name, **k: self.calls[_n].append((a, k)))
            p.start()
            self.addCleanup(p.stop)

    def loader(self, result):
        @monitoring.monitored(dataset="standings", target_table="raw.raw_league_standings")
        def load(rows, *, league, source="nightly"):
            return result
        return load

    def bad(self):
        return [{"team_name": "Club Leon", dlq.DLQ_REASON: "null_team_id"}]

    def test_first_parking_records_opens_event_and_alerts_once(self):
        res = dlq.LoadResult(loaded=17, parked=self.bad(), dlq_key="dlq/standings/262/2026/open.json", new_entry=True)
        self.loader(res)([], league={"api_league_id": 262, "season": 2026})
        movement = self.calls["movement"][0][1]
        self.assertEqual((movement["movement_type"], movement["status"], movement["failed_count"]),
                         ("row_validation", "partial", 1))
        self.assertEqual(movement["details"]["parked_rows"], ["Club Leon"])
        self.assertEqual(self.calls["event"][0][1]["check_name"], "unprocessed_rows")
        self.assertIn("1 unprocessable row(s) parked", self.calls["alert"][0][0][0])

    def test_repeat_sighting_records_only(self):
        res = dlq.LoadResult(loaded=17, parked=self.bad(), dlq_key="k", new_entry=False)
        self.loader(res)([], league={"api_league_id": 262, "season": 2026})
        self.assertEqual((len(self.calls["movement"]), self.calls["event"], self.calls["alert"]), (1, [], []))

    def test_dlq_write_failure_is_an_error_alert(self):
        res = dlq.LoadResult(loaded=17, parked=self.bad(), dlq_write_failed=True)
        self.loader(res)([], league={"api_league_id": 262, "season": 2026})
        self.assertEqual(self.calls["event"][0][1]["severity"], "error")
        self.assertIn("DLQ write failed", self.calls["alert"][0][0][0])

    def test_resolved_closes_event(self):
        self.loader(dlq.LoadResult(loaded=18, resolved=True))([], league={"api_league_id": 262, "season": 2026})
        self.assertEqual(self.calls["resolve"][0][1]["check_name"], "unprocessed_rows")
        self.assertEqual(self.calls["movement"][0][1]["status"], "success")

    def test_monitoring_failure_never_fails_the_load(self):
        with patch("etl.src.data_detector.data_movements.record_data_movement", side_effect=RuntimeError("db")):
            res = self.loader(dlq.LoadResult(loaded=3))([], league={})
        self.assertEqual(res.loaded, 3)


class DictCursor:
    """Returns queued rows for the decorators' queries."""

    def __init__(self, *rows):
        self.rows, self.sql = list(rows), []

    def execute(self, sql, params=None):
        self.sql.append(sql)

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


def page(**extra):
    base = {"fixture": {"fixture_id": 7, "run_ds": "2026-10-19",
                        "standings_updated_at": "2026-10-19T00:10:00+00:00"},
            "context": {"table": {"incomplete": False, "groups": []}}}
    base.update(extra)
    return base


NOW = lambda: datetime(2026, 10, 20, 12, tzinfo=timezone.utc)   # noqa: E731


class DataDecoratorTests(unittest.TestCase):
    def build(self, payload, cur=None, *decorators):
        comp = dq.decorate(dq.Component(lambda c, fixture_id: payload), *decorators)
        return comp.build(cur or DictCursor(), fixture_id=7)

    def test_clean_page_is_ok(self):
        p = self.build(page(), None, dq.StandingsQuality, lambda inner: dq.Freshness(inner, NOW))
        self.assertEqual(p["data_quality"], {"ok": True, "issues": []})

    def test_standings_incomplete_names_the_stale_team(self):
        ctx = {"table": {"incomplete": True, "unprocessed_rows": 1, "groups": [
            {"rows": [{"team_name": "Toluca", "stale": False}, {"team_name": "Club Leon", "stale": True}]}]}}
        p = self.build(page(context=ctx), None, dq.StandingsQuality)
        issue = p["data_quality"]["issues"][0]
        self.assertEqual((issue["code"], issue["section"]), ("standings_incomplete", "context"))
        self.assertIn("Club Leon", issue["message"])
        self.assertFalse(p["data_quality"]["ok"])

    def test_fixture_rows_issue_from_status_table(self):
        cur = DictCursor({"ready": True}, {"status": "unprocessed", "unprocessed_rows": 2,
                                           "unprocessed_since": datetime(2026, 10, 19, tzinfo=timezone.utc)})
        p = self.build(page(), cur, dq.FixtureRowsQuality)
        issue = p["data_quality"]["issues"][0]
        self.assertEqual((issue["code"], issue["since"]), ("fixtures_incomplete", "2026-10-19T00:00:00+00:00"))
        self.assertIn("2 fixtures", issue["message"])

    def test_fixture_rows_quiet_without_table_or_issue(self):
        for cur in (DictCursor({"ready": False}), DictCursor({"ready": True}, None)):
            self.assertTrue(self.build(page(), cur, dq.FixtureRowsQuality)["data_quality"]["ok"])

    def test_freshness(self):
        old = page(fixture={"run_ds": "2026-10-15", "standings_updated_at": "2026-10-17T00:10:00+00:00"})
        p = self.build(old, None, lambda inner: dq.Freshness(inner, NOW))
        self.assertEqual([i["code"] for i in p["data_quality"]["issues"]], ["standings_stale", "engine_stale"])

    def test_broken_decorator_never_breaks_the_page(self):
        class Boom(dq.DataDecorator):
            def annotate(self, payload, cur, **kw):
                raise RuntimeError("db gone")
        p = self.build(page(), None, Boom, dq.StandingsQuality)
        self.assertTrue(p["data_quality"]["ok"])

    def test_unknown_fixture_stays_none(self):
        comp = dq.decorate(dq.Component(lambda c, fixture_id: None), dq.StandingsQuality)
        self.assertIsNone(comp.build(DictCursor(), fixture_id=1))

    def test_match_page_carries_the_block(self):
        from engine.api import match_detail as md
        with patch.object(md, "build_match_core", return_value=page()), \
                patch.object(dq.Freshness, "annotate", return_value=[]):
            p = md.build_match(DictCursor({"ready": False}), 7)
        self.assertIn("data_quality", p)


@unittest.skipUnless(os.getenv("FLOW_TEST_DSN"), "requires FLOW_TEST_DSN (throwaway database)")
class PostgresFixtureTests(unittest.TestCase):
    def setUp(self):
        import psycopg2
        from etl.src.s3_landing import RAW_COLUMNS
        self.conn = psycopg2.connect(os.environ["FLOW_TEST_DSN"])
        cur = self.conn.cursor()
        cols = ", ".join(f"{c} TEXT" for c in RAW_COLUMNS if c not in
                         ("api_fixture_id", "api_league_id", "season", "kickoff_utc", "fixture_status",
                          "home_team_id", "away_team_id"))
        cur.execute(f"""CREATE SCHEMA IF NOT EXISTS raw;
            DROP TABLE IF EXISTS raw.raw_fixtures, raw.raw_fixtures_status;
            CREATE TABLE raw.raw_fixtures (fixture_id BIGSERIAL PRIMARY KEY, api_fixture_id BIGINT NOT NULL UNIQUE,
              api_league_id INT NOT NULL, season INT NOT NULL, kickoff_utc TIMESTAMPTZ NOT NULL,
              fixture_status TEXT NOT NULL, home_team_id INT, away_team_id INT, {cols},
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
            INSERT INTO raw.raw_fixtures (api_fixture_id, api_league_id, season, kickoff_utc, fixture_status,
              home_team_id, away_team_id, home_team_name, updated_at)
            VALUES (1, 262, 2026, '2026-10-18', 'FT', 10, 11, 'old', '2026-10-01'),
                   (2, 262, 2026, '2026-10-18', 'FT', 12, 13, 'last good', '2026-10-01');""")
        self.conn.commit()
        self.s3 = FakeS3()
        self.s3.install(self)
        p = patch("etl.src.data_detector.monitoring.record", lambda *a, **k: None)
        p.start()
        self.addCleanup(p.stop)

    def tearDown(self):
        self.conn.close()

    def rows(self):
        cur = self.conn.cursor()
        cur.execute("SELECT api_fixture_id, home_team_name FROM raw.raw_fixtures ORDER BY 1")
        return cur.fetchall()

    def test_bad_row_no_longer_fails_the_copy_and_keeps_last_good(self):
        with patch.object(fd, "set_status", lambda *a, **k: None):
            fd.load_league_fixtures([dict(fx(1), home_team_name="new"), fx(2, kickoff=None), fx(3)],
                                    league=LEAGUE, ds="2026-10-19", conn=self.conn)
        self.conn.commit()
        self.assertEqual(self.rows(), [(1, "new"), (2, "last good"), (3, "H3")])

    def ids(self):
        cur = self.conn.cursor()
        cur.execute("SELECT api_fixture_id, fixture_id FROM raw.raw_fixtures ORDER BY 1")
        return dict(cur.fetchall())

    def load(self, rows):
        with patch.object(fd, "set_status", lambda *a, **k: None):
            fd.load_league_fixtures(rows, league=LEAGUE, ds="2026-10-19", conn=self.conn, source="replay")
        self.conn.commit()

    def test_clean_reload_keeps_fixture_ids(self):
        before = self.ids()
        self.load([dict(fx(1), home_team_name="new"), fx(2), fx(3)])
        after = self.ids()
        self.assertEqual((after[1], after[2]), (before[1], before[2]))   # downstream keys still valid
        self.assertEqual(self.rows(), [(1, "new"), (2, "H2"), (3, "H3")])
        cur = self.conn.cursor()
        cur.execute("SELECT bool_and(updated_at > '2026-10-02') FROM raw.raw_fixtures")
        self.assertTrue(cur.fetchone()[0])                          # dbt's incremental staging sees them

    def test_clean_reload_drops_fixtures_the_api_dropped(self):
        self.load([fx(1)])
        self.assertEqual([r[0] for r in self.rows()], [1])

    def test_repeated_reloads_keep_ids_and_sequence(self):
        self.load([fx(1), fx(2), fx(3)])
        first = self.ids()
        for _ in range(3):
            self.load([fx(1), fx(2), fx(3)])
        self.assertEqual(self.ids(), first)
        self.load([fx(1), fx(2), fx(3), fx(4)])
        self.assertEqual(self.ids()[4], max(first.values()) + 1)    # no sequence burnt by reloads

    def test_repeated_id_in_one_response_loads_once_last_wins(self):
        self.load([dict(fx(3), home_team_name="first"), fx(1), fx(2), dict(fx(3), home_team_name="second")])
        self.assertEqual(self.rows(), [(1, "H1"), (2, "H2"), (3, "second")])

    def test_status_row_and_daily_check(self):
        from etl.src.data_detector import daily_flow as flow
        bad = [dict(fx(2, kickoff=None), **{dlq.DLQ_REASON: "null_kickoff"})]
        fd.set_status(LEAGUE, bad, (KEY, dlq.new_state(), True), conn=self.conn)
        self.conn.commit()
        with self.conn.cursor() as cur:
            res = flow.check_dlq(cur, {"type": "dlq", "source": "fixture_rows"}, date(2026, 10, 19))
        self.assertEqual((res["status"], res.get("severity")), ("failed", "warning"))
        self.assertIn("H2 v A2", res["details"]["entries"][0])
        fd.set_status(LEAGUE, [], None, conn=self.conn)
        self.conn.commit()
        with self.conn.cursor() as cur:
            self.assertEqual(flow.check_dlq(cur, {"type": "dlq", "source": "fixture_rows"},
                                            date(2026, 10, 19))["status"], "ok")


if __name__ == "__main__":
    unittest.main()
