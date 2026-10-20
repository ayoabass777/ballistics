"""DLQ (D147): retry policy, @dead_letter, standings parking/replay, fixture
retries, the incomplete-table flag and the daily-check monitoring."""
import os
import unittest
from datetime import date, timedelta
from unittest.mock import patch

from etl.src import dlq
from etl.src import replay_dlq
from etl.src import standings_dlq as sd

LEAGUE = {"league_season_id": 1870, "api_league_id": 262, "season": 2026}
KEY = "dlq/standings/262/2026/open.json"


def _api_row(team_id, name, rank):
    return {"rank": rank, "team": {"id": team_id, "name": name}, "points": 18 - rank,
            "goalsDiff": 1, "form": "WDW", "group": "Liga MX: Apertura", "description": None,
            "all": {"played": 11, "win": 5, "draw": 3, "lose": 3, "goals": {"for": 15, "against": 12}}}


def _payload(leon_id=None):
    return [{"league": {"standings": [[_api_row(10, "Toluca", 1), _api_row(11, "America", 2),
                                       _api_row(leon_id, "Club Leon", 5)]]}}]


class FakeS3:
    """In-memory stand-in for the dlq module's S3 helpers."""

    def __init__(self):
        self.objects = {}

    def read_json(self, key, bucket=None):
        return self.objects.get(key)

    def write_json(self, key, payload, bucket=None):
        self.objects[key] = dict(payload)
        return key

    def delete_key(self, key, bucket=None):
        self.objects.pop(key, None)

    def list_keys(self, prefix, bucket=None):
        return sorted(k for k in self.objects if k.startswith(prefix))

    def install(self, test):
        for name in ("read_json", "write_json", "delete_key", "list_keys"):
            p = patch.object(dlq, name, getattr(self, name))
            p.start()
            test.addCleanup(p.stop)
        p = patch.object(dlq, "default_bucket", lambda: "test-bucket")
        p.start()
        test.addCleanup(p.stop)


class RetryPolicyTests(unittest.TestCase):
    def test_new_entry_waits_an_hour(self):
        now = dlq.now_utc()
        entry = dlq.new_state(now)
        self.assertFalse(dlq.is_due(entry, now))
        self.assertTrue(dlq.is_due(entry, now + timedelta(hours=1)))

    def test_failed_attempt_reschedules_then_dies_at_24(self):
        now = dlq.now_utc()
        entry = dlq.new_state(now)
        for i in range(1, dlq.MAX_ATTEMPTS):
            entry, dead = dlq.record_failed_attempt(entry, "still bad", now)
            self.assertFalse(dead)
            self.assertEqual(entry["attempts"], i)
            self.assertEqual(entry["next_retry_at"], dlq._iso(now + timedelta(hours=1)))
        entry, dead = dlq.record_failed_attempt(entry, "still bad", now)
        self.assertTrue(dead)
        self.assertEqual((entry["status"], entry["attempts"], entry["next_retry_at"]), ("dead", 24, None))
        self.assertFalse(dlq.is_due(entry, now + timedelta(days=2)))
        _, dead_again = dlq.record_failed_attempt(entry, "x", now)
        self.assertFalse(dead_again)                      # alert once, not every hour

    def test_entry_written_before_d144_is_open_and_due(self):
        legacy = {"api_league_id": 39, "error": "timeout", "timestamp": "2026-10-10T00:00:00+00:00"}
        self.assertTrue(dlq.is_due(legacy))
        self.assertEqual(dlq.with_state(legacy)["first_seen"], "2026-10-10T00:00:00+00:00")

    def test_archive_paths(self):
        now = dlq._parse("2026-10-20T01:00:00+00:00")
        self.assertEqual(dlq.archive_key(KEY, now), "dlq/resolved/standings/262/2026/20261020T0100Z.json")
        self.assertEqual(dlq.archive_key("dlq/39/2026/2026-10-15/error.json", now, group="fixtures"),
                         "dlq/resolved/fixtures/39/2026/2026-10-15/20261020T0100Z.json")


class DeadLetterDecoratorTests(unittest.TestCase):
    def test_bad_rows_go_to_park_and_loader_gets_the_rest(self):
        parked_calls = []

        @dlq.dead_letter(validate=lambda r: None if r["id"] else "no_id",
                         park=lambda bad, *a, **kw: parked_calls.append((bad, kw)) or "receipt")
        def load(rows, *, league, quarantined=(), parked=None):
            return rows, quarantined, parked

        good, bad, parked = load([{"id": 1}, {"id": None}], league="L")
        self.assertEqual(good, [{"id": 1}])
        self.assertEqual(bad, [{"id": None, dlq.DLQ_REASON: "no_id"}])
        self.assertEqual(parked, "receipt")
        self.assertEqual(parked_calls[0][1], {"league": "L"})

    def test_clean_rows_never_call_park(self):
        @dlq.dead_letter(validate=lambda r: None, park=lambda *a, **k: self.fail("parked"))
        def load(rows, quarantined=()):
            return len(rows), quarantined

        self.assertEqual(load([{"a": 1}]), (1, []))


class StandingsDlqTests(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.s3.install(self)
        self.upserts, self.statuses, self.alerts = [], [], []
        for target, fn in (
            ("etl.src.update_standings.upsert_standings",
             lambda rows, prune=True: self.upserts.append((rows, prune)) or len(rows)),
            ("etl.src.standings_dlq.set_status",
             lambda league, bad, parked, conn=None: self.statuses.append((len(bad), parked))),
            ("etl.src.dlq.alert_dead", lambda *a, **k: self.alerts.append(a)),
        ):
            p = patch(target, fn)
            p.start()
            self.addCleanup(p.stop)

    def nightly(self, leon_id=None):
        return sd.refresh_league(LEAGUE, "2026-10-17", payload=_payload(leon_id))

    def test_null_team_id_is_parked_rest_loads_without_prune(self):
        inserted, bad = self.nightly()
        self.assertEqual(inserted, 2)
        rows, prune = self.upserts[0]
        self.assertFalse(prune)                                       # León's last good row stays
        self.assertEqual([r["team_name"] for r in rows], ["Toluca", "America"])
        entry = self.s3.objects[KEY]
        self.assertEqual((entry["status"], entry["attempts"], entry["teams"]), ("open", 0, ["Club Leon"]))
        self.assertEqual(entry["reasons"], ["null_team_id"])
        self.assertEqual(entry["rows"][0]["rank"], 5)                 # kept as the API sent it
        self.assertEqual(self.statuses[-1][0], 1)

    def test_replay_still_bad_counts_an_attempt(self):
        self.nightly()
        self.s3.objects[KEY]["next_retry_at"] = dlq._iso(dlq.now_utc() - timedelta(minutes=1))
        with patch("etl.src.update_standings.fetch_standings", return_value=_payload()):
            summary = sd.replay_standings_dlq()
        self.assertEqual(summary["still_bad"], 1)
        self.assertEqual(self.s3.objects[KEY]["attempts"], 1)

    def test_entry_first_seen_by_replay_starts_at_zero(self):
        parked = sd.park_standings_rows([{"api_team_id": None, "team_name": "Club Leon",
                                          dlq.DLQ_REASON: "null_team_id"}],
                                        league=LEAGUE, ds="2026-10-20", source="replay")
        self.assertEqual(parked[1]["attempts"], 0)                    # 24 retries left, not 23

    def test_replay_not_due_is_skipped(self):
        self.nightly()
        with patch("etl.src.update_standings.fetch_standings") as fetch:
            summary = sd.replay_standings_dlq()
        fetch.assert_not_called()
        self.assertEqual(summary["skipped"], 1)

    def test_replay_clean_resolves_and_prunes(self):
        self.nightly()
        self.s3.objects[KEY]["next_retry_at"] = None
        with patch("etl.src.update_standings.fetch_standings", return_value=_payload(leon_id=12)):
            summary = sd.replay_standings_dlq()
        self.assertEqual(summary["resolved"], 1)
        self.assertNotIn(KEY, self.s3.objects)
        archived = self.s3.list_keys("dlq/resolved/standings/262/2026/")
        self.assertEqual(len(archived), 1)
        self.assertEqual(self.s3.objects[archived[0]]["resolved_by"], "replay")
        self.assertTrue(self.upserts[-1][1])                          # prune back on
        self.assertEqual(self.statuses[-1], (0, None))                # flag cleared

    def test_clean_nightly_run_resolves_a_dead_entry(self):
        self.nightly()
        self.s3.objects[KEY]["status"] = "dead"
        self.nightly(leon_id=12)
        self.assertNotIn(KEY, self.s3.objects)
        self.assertEqual(self.s3.objects[self.s3.list_keys("dlq/resolved/")[0]]["resolved_by"], "nightly")

    def test_gives_up_after_24_replays_and_alerts_once(self):
        self.nightly()
        with patch("etl.src.update_standings.fetch_standings", return_value=_payload()):
            for _ in range(30):
                if self.s3.objects[KEY]["status"] == "open":
                    self.s3.objects[KEY]["next_retry_at"] = None
                sd.replay_standings_dlq()
        entry = self.s3.objects[KEY]
        self.assertEqual((entry["status"], entry["attempts"]), ("dead", 24))
        self.assertEqual(len(self.alerts), 1)

    def test_empty_replay_response_counts_an_attempt(self):
        self.nightly()
        self.s3.objects[KEY]["next_retry_at"] = None
        with patch("etl.src.update_standings.fetch_standings", return_value=[]):
            sd.replay_standings_dlq()
        self.assertEqual(self.s3.objects[KEY]["attempts"], 1)
        self.assertIn("empty", self.s3.objects[KEY]["last_error"])

    def test_s3_down_still_loads_and_flags_without_key(self):
        with patch.object(dlq, "read_json", side_effect=OSError("network down")):
            inserted, bad = self.nightly()
        self.assertEqual((inserted, len(bad)), (2, 1))
        self.assertEqual(self.statuses[-1], (1, None))                # daily check: error


class FixtureRetryTests(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.s3.install(self)
        self.alerts = []
        p = patch("etl.src.dlq.alert_dead", lambda *a, **k: self.alerts.append(a))
        p.start()
        self.addCleanup(p.stop)

    def listed(self, key, ds="2026-10-15"):
        return {"api_league_id": 39, "season_year": 2026, "ds": ds, "error_key": key}

    def test_only_due_entries_replay(self):
        now = dlq.now_utc()
        self.s3.objects["dlq/39/2026/2026-10-15/error.json"] = {"error": "x", **dlq.new_state(now)}
        self.s3.objects["dlq/39/2026/2026-10-16/error.json"] = {"error": "legacy, no state"}
        due = replay_dlq.due_fixture_entries([self.listed("dlq/39/2026/2026-10-15/error.json"),
                                              self.listed("dlq/39/2026/2026-10-16/error.json", "2026-10-16")])
        self.assertEqual([e["ds"] for e in due], ["2026-10-16"])

    def test_replay_resolves_the_entry_only_after_commit(self):
        key = "dlq/39/2026/2026-10-15/error.json"
        self.s3.objects[key] = {"error": "x", **dlq.new_state()}
        conn = object.__new__(type("Conn", (), {}))
        with patch.object(replay_dlq, "fetch_fixtures", return_value={}), \
                patch.object(replay_dlq, "extract_fixtures_field", return_value=[{"id": 1}]), \
                patch.object(replay_dlq, "write_fixtures_to_s3", return_value="full_load/39/2026/d/f.json"), \
                patch.object(replay_dlq, "fixture_kickoff_watermark", return_value=(None, None, None)), \
                patch.object(replay_dlq, "load_fixtures_from_s3", return_value=1):
            self.assertTrue(replay_dlq.replay_from_dlq(self.listed(key), conn))
        self.assertIn(key, self.s3.objects)                         # caller hasn't committed
        dlq.run_after_commit(conn)
        self.assertNotIn(key, self.s3.objects)
        self.assertEqual(len(self.s3.list_keys("dlq/resolved/fixtures/")), 1)

    def test_failed_replays_die_at_24_with_one_alert(self):
        key = "dlq/39/2026/2026-10-15/error.json"
        self.s3.objects[key] = {"error": "timeout", **dlq.new_state()}
        dead = [replay_dlq.fixture_attempt_failed(self.listed(key), "replay returned no rows")
                for _ in range(24)]
        self.assertEqual(dead.count(True), 1)
        self.assertEqual(self.s3.objects[key]["status"], "dead")
        self.assertEqual(len(self.alerts), 1)

    def test_dead_extraction_is_rechecked_daily_without_realerting(self):
        key = "dlq/39/2026/2026-10-15/error.json"
        long_ago = dlq._iso(dlq.now_utc() - timedelta(hours=25))
        self.s3.objects[key] = {"error": "timeout", **dlq.new_state(), "status": "dead",
                                "attempts": 24, "last_attempt_at": long_ago, "next_retry_at": None}
        self.assertEqual(len(replay_dlq.due_fixture_entries([self.listed(key)])), 1)
        self.assertFalse(replay_dlq.fixture_attempt_failed(self.listed(key), "still failing"))
        self.assertEqual((self.s3.objects[key]["status"], self.alerts), ("dead", []))
        self.assertEqual(replay_dlq.due_fixture_entries([self.listed(key)]), [])   # tomorrow


class MatchPageFlagTests(unittest.TestCase):
    def test_stale_row_and_incomplete_reach_the_payload(self):
        from engine.api import match_context as mc
        base = {"group_name": "Apertura", "group_order": 0, "games_played": 11, "goal_diff": 1,
                "description": None, "n_groups": 1, "ranks_repeat": False, "wins": 5, "draws": 3,
                "losses": 3, "form": "WDW", "is_team_current_group": True,
                "standings_incomplete": True, "unprocessed_rows": 1}
        rows = [dict(base, team_id=1, team_name="Toluca", rank=1, points=18, is_stale_row=False),
                dict(base, team_id=2, team_name="Club Leon", rank=5, points=18, is_stale_row=True)]
        groups = mc._groups(rows)
        self.assertEqual([r["stale"] for r in groups[0]["rows"]], [False, True])
        self.assertTrue(mc.team_position(rows, 2)["stale"])


class FakeStatusCursor:
    """Status table as the daily check reads it: to_regclass, then flagged rows."""

    def __init__(self, rows=None):
        self.rows = rows          # None = table not created yet
        self._next = None

    def execute(self, sql, params=None):
        if "to_regclass" in sql:
            self._next = [(None if self.rows is None else "raw.status",)]
        else:
            self._next = list(self.rows or [])

    def fetchone(self):
        return self._next[0]

    def fetchall(self):
        return self._next


class S3CrossCheckTests(unittest.TestCase):
    """Parked on S3 but the status flag write failed: the daily check must still see it."""

    def setUp(self):
        self.s3 = FakeS3()
        self.s3.install(self)

    def check(self, cur, source="standings"):
        from etl.src.data_detector import daily_flow as flow
        return flow.check_dlq(cur, {"type": "dlq", "source": source}, date(2026, 10, 19))

    def test_unflagged_entry_is_an_error_even_without_status_table(self):
        self.s3.objects[KEY] = {"kind": "standings", **dlq.new_state()}
        res = self.check(FakeStatusCursor(rows=None))
        self.assertEqual((res["status"], res.get("severity")), ("failed", None))   # error
        self.assertIn("status write failed", res["details"]["entries"][0])

    def test_flagged_entry_is_not_reported_twice(self):
        self.s3.objects[KEY] = {"kind": "standings", **dlq.new_state()}
        res = self.check(FakeStatusCursor(rows=[(262, 2026, "unprocessed", ["Club Leon"], 1.0, 0, KEY)]))
        self.assertEqual((res["status"], res.get("severity")), ("failed", "warning"))
        self.assertEqual(len(res["details"]["entries"]), 1)
        self.assertNotIn("status write failed", res["details"]["entries"][0])

    def test_resolved_archive_is_ignored(self):
        self.s3.objects["dlq/resolved/standings/262/2026/20261019T0100Z.json"] = {"status": "resolved"}
        self.assertEqual(self.check(FakeStatusCursor(rows=[]))["status"], "ok")

    def test_fixture_rows_prefix(self):
        self.s3.objects["dlq/fixtures/rows/39/2026/open.json"] = {"kind": "fixture_rows", **dlq.new_state()}
        res = self.check(FakeStatusCursor(rows=[]), source="fixture_rows")
        self.assertIsNone(res.get("severity"))
        self.assertIn("league 39/2026", res["details"]["entries"][0])

    def test_s3_down_keeps_db_side_and_says_so(self):
        with patch.object(dlq, "list_keys", side_effect=OSError("Name or service not known")):
            res = self.check(FakeStatusCursor(rows=[(262, 2026, "unprocessed", ["Club Leon"], 1.0, 0, KEY)]))
        self.assertEqual(res.get("severity"), "warning")
        self.assertTrue(res["details"]["s3_cross_check"].startswith("skipped"))


@unittest.skipUnless(os.getenv("FLOW_TEST_DSN"), "requires FLOW_TEST_DSN (throwaway database)")
class PostgresDlqTests(unittest.TestCase):
    def setUp(self):
        import psycopg2
        self.conn = psycopg2.connect(os.environ["FLOW_TEST_DSN"])
        cur = self.conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw; DROP TABLE IF EXISTS raw.raw_league_standings_status")
        self.conn.commit()
        self.s3 = FakeS3()
        self.s3.install(self)

    def tearDown(self):
        self.conn.close()

    def status(self):
        cur = self.conn.cursor()
        cur.execute("SELECT status, unprocessed_rows, unprocessed_teams, unprocessed_since, dlq_key "
                    "FROM raw.raw_league_standings_status WHERE league_season_id = 1870")
        return cur.fetchone()

    def bad(self):
        return [{"team_name": "Club Leon", dlq.DLQ_REASON: "null_team_id"}]

    def test_flag_keeps_first_seen_and_clears_on_clean_refresh(self):
        entry = {**dlq.new_state(), "attempts": 2}
        sd.set_status(LEAGUE, self.bad(), (KEY, entry), conn=self.conn)
        first = self.status()
        self.assertEqual(first[:3], ("unprocessed", 1, ["Club Leon"]))
        self.assertEqual(first[4], KEY)
        sd.set_status(LEAGUE, self.bad(), (KEY, {**entry, "status": "dead"}), conn=self.conn)
        again = self.status()
        self.assertEqual((again[0], again[3]), ("dead", first[3]))     # since = first sighting
        sd.set_status(LEAGUE, [], None, conn=self.conn)
        self.assertEqual(self.status(), ("ok", 0, None, None, None))

    def check(self, source="standings"):
        from etl.src.data_detector import daily_flow as flow
        with self.conn.cursor() as cur:
            return flow.check_dlq(cur, {"type": "dlq", "source": source}, date(2026, 10, 19))

    def test_daily_check_levels(self):
        self.assertEqual(self.check()["status"], "ok")                 # table not created yet
        sd.set_status(LEAGUE, [], None, conn=self.conn)
        self.assertEqual(self.check()["status"], "ok")
        sd.set_status(LEAGUE, self.bad(), (KEY, dlq.new_state()), conn=self.conn)
        young = self.check()
        self.assertEqual((young["status"], young.get("severity")), ("failed", "warning"))
        self.assertIn("Club Leon", young["details"]["entries"][0])
        cur = self.conn.cursor()
        cur.execute("UPDATE raw.raw_league_standings_status SET unprocessed_since = now() - interval '25 hours'")
        self.conn.commit()
        old = self.check()
        self.assertEqual((old["status"], old.get("severity")), ("failed", None))   # error
        sd.set_status(LEAGUE, self.bad(), None, conn=self.conn)           # S3 write failed
        cur.execute("UPDATE raw.raw_league_standings_status SET unprocessed_since = now()")
        self.conn.commit()
        lost = self.check()
        self.assertIsNone(lost.get("severity"))
        self.assertIn("write failed", lost["details"]["entries"][0])

    def test_daily_check_fixtures(self):
        s3 = self.s3
        key = "dlq/39/2026/2026-10-15/error.json"
        s3.objects[key] = {"error": "x", **dlq.new_state()}
        listed = [{"api_league_id": 39, "season_year": 2026, "ds": "2026-10-15", "error_key": key}]
        with patch("etl.src.s3_landing.list_dlq_entries", return_value=listed):
            self.assertEqual(self.check("fixtures").get("severity"), "warning")
            s3.objects[key]["status"] = "dead"
            self.assertIsNone(self.check("fixtures").get("severity"))
        with patch("etl.src.s3_landing.list_dlq_entries", side_effect=OSError("Name or service not known")):
            down = self.check("fixtures")
        self.assertEqual((down.get("severity"), down["details"]["reason"]),
                         ("warning", "network down: DLQ not checked"))


if __name__ == "__main__":
    unittest.main()
