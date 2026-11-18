"""Live results feed raw_fixtures; the nightly sweep checks them (etl/src/live_results.py)."""
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from etl.src import live_poller as lp
from etl.src import live_results as lr
from etl.src import update_fixtures
from etl.src.fixtures_dlq import fixture_row_problem

NOW = datetime(2026, 11, 16, 21, 0, tzinfo=timezone.utc)


def api_item(api_id=500, status="FT", home=2, away=1, ht=(1, 0)):
    return {"fixture": {"id": api_id, "date": "2026-11-16T19:00:00+00:00",
                        "status": {"short": status, "elapsed": 90, "extra": None}},
            "league": {"id": 39, "season": 2026, "round": "Regular Season - 12"},
            "teams": {"home": {"id": 10, "name": "Brann"}, "away": {"id": 20, "name": "Molde"}},
            "goals": {"home": home, "away": away},
            "score": {"halftime": {"home": ht[0], "away": ht[1]}, "fulltime": {"home": home, "away": away}},
            "events": []}


class FakeCursor:
    """Returns queued fetchall results in order; records every execute."""
    def __init__(self, *results):
        self.results, self.executed = list(results), []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.results.pop(0) if self.results else []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self, cur):
        self.cur, self.commits, self.closed = cur, 0, False

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class PromotableTests(unittest.TestCase):
    def test_only_finals_with_both_full_time_goals(self):
        rows = lr.promotable([api_item(1), api_item(2, status="2H"), api_item(3, home=None),
                              api_item(4, status="AET"), api_item(5, status="PEN")])
        self.assertEqual([r["api_fixture_id"] for r in rows], [1, 4, 5])
        self.assertEqual((rows[0]["home_team_fulltime_goal"], rows[0]["home_fulltime_result"]), (2, "win"))

    def test_missing_half_time_is_allowed(self):
        # 848 local finals have no half-time score; a full-time one is enough.
        self.assertEqual(len(lr.promotable([api_item(ht=(None, None))])), 1)

    def test_every_load_rejects_a_final_without_a_score(self):
        row = lr.promotable([api_item()])[0]
        self.assertIsNone(fixture_row_problem(row))
        self.assertEqual(fixture_row_problem({**row, "home_team_fulltime_goal": None}), "final_without_score")
        self.assertIsNone(fixture_row_problem({**row, "fixture_status": "NS", "home_team_fulltime_goal": None,
                                               "away_team_fulltime_goal": None}))


class PromoterTests(unittest.TestCase):
    def test_lands_in_s3_before_loading_then_marks_promoted(self):
        calls, cur = [], FakeCursor()
        promoter = lr.Promoter(lambda: FakeConn(cur),
                               write=lambda rows, now: calls.append("s3") or "incremental/live/k.json",
                               apply=lambda rows: calls.append("load") or len(rows),
                               record=lambda rows, key, n: calls.append("audit"))
        self.assertEqual(promoter([api_item(), api_item(2, status="2H")], NOW), 1)
        self.assertEqual(calls, ["s3", "load", "audit"])
        sql, params = cur.executed[0]
        self.assertIn("promoted_at", sql)
        self.assertEqual((params["api_fixture_id"], params["fixture_status"],
                          params["home_team_fulltime_goal"], params["away_team_fulltime_goal"]), (500, "FT", 2, 1))

    def test_nothing_final_writes_nothing(self):
        promoter = lr.Promoter(lambda: self.fail("no db"), write=lambda *a: self.fail("no s3"),
                               apply=lambda *a: self.fail("no load"), record=lambda *a: None)
        self.assertEqual(promoter([api_item(status="HT")], NOW), 0)

    def test_failed_s3_write_does_not_load(self):
        def boom(rows, now):
            raise RuntimeError("s3 down")
        promoter = lr.Promoter(lambda: self.fail("no db"), write=boom,
                               apply=lambda *a: self.fail("must not load"), record=lambda *a: None)
        with self.assertRaises(RuntimeError):
            promoter([api_item()], NOW)

    def test_s3_keys_are_never_reused(self):
        self.assertEqual(lr.live_s3_key(NOW), "incremental/live/2026-11-16/210000000000.json")


class FakeProvider:
    def __init__(self, ids=()):
        self._ids, self.remaining = list(ids), 5000

    def live(self):
        return []

    def by_ids(self, api_ids):
        return self._ids


class FakeStore:
    def __init__(self, open_live):
        self._open = open_live

    def window(self, now): return []
    def open_fixtures(self): return self._open
    def next_kickoff(self, now): return None
    def fixture_ids(self, api_ids): return {}
    def write_poll(self, parsed, now): return [s["fixture_id"] for s, _ in parsed]


class PollerPromotionTests(unittest.TestCase):
    def test_promotes_only_finals_from_the_by_id_fetch(self):
        promoted = []
        provider = FakeProvider(ids=[api_item(500), api_item(501, status="PST", home=None, away=None)])
        p = lp.Poller(provider, FakeStore({7: 500, 8: 501}), lambda *a: None, clock=lambda: NOW,
                      promote=lambda items, now: promoted.append([i["fixture"]["id"] for i in items]))
        p.cycle()
        self.assertEqual(promoted, [[500]])

    def test_failed_promotion_alerts_and_polling_continues(self):
        alerts = []

        def boom(items, now):
            raise RuntimeError("db down")
        p = lp.Poller(FakeProvider(ids=[api_item(500)]), FakeStore({7: 500}), lambda s, b: alerts.append(s),
                      clock=lambda: NOW, promote=boom)
        self.assertEqual(p.cycle(), lp.POLL_SECONDS)
        self.assertEqual(p.failures, 0)
        self.assertEqual(alerts, ["✗ Ballistics live scores: results not written"])


class SweepTests(unittest.TestCase):
    def rows(self):
        return lr.promotable([api_item(500), api_item(501, home=1, away=1), api_item(502)])

    def test_rewrites_only_changed_results(self):
        # 500 agrees with what was promoted, 501 was promoted as 2-1 and is now
        # 1-1, 502 was never promoted (a normal nightly catch-up).
        cur = FakeCursor([(500, "FT", 2, 1), (501, "FT", 2, 1)])
        apply, stamps, changed = lr.sweep(cur, self.rows())
        self.assertEqual([r["api_fixture_id"] for r in apply], [501, 502])
        self.assertEqual(sorted(stamps), [(500, False), (501, True)])
        self.assertEqual(changed, ["Brann v Molde (api 501): FT 2-1 → FT 1-1"])

    def test_payload_without_promoted_fixtures_is_untouched(self):
        rows = self.rows()
        self.assertEqual(lr.sweep(FakeCursor([]), rows), (rows, [], []))

    def test_mark_swept(self):
        cur = FakeCursor()
        lr.mark_swept(cur, [(500, False), (501, True)], now=NOW)
        self.assertEqual([p["changed"] for _, p in cur.executed], [False, True])

    def test_nightly_work_list_adds_unswept_results_once(self):
        cur = FakeCursor([(42, 1532879)], [(42, 1532879), (77, 900001)])
        with patch.object(update_fixtures, "get_db_connection", return_value=FakeConn(cur)):
            ids = update_fixtures.to_update_fixture_ids()
        self.assertEqual(ids, [(42, "1532879"), (77, "900001")])
        self.assertIn("swept_at IS NULL", cur.executed[1][0])


class SchemaCheckLockTests(unittest.TestCase):
    """The loader runs during matchdays now; it must not take an exclusive
    lock on raw_fixtures when there is nothing to add."""
    def run_check(self, found):
        from etl.src.schema_checks import migrate_raw_fixtures_columns
        cur = FakeCursor()
        cur.fetchone = lambda: found
        migrate_raw_fixtures_columns(cur)
        return [sql for sql, _ in cur.executed if "ALTER TABLE" in sql]

    def test_no_alter_when_the_column_exists(self):
        self.assertEqual(self.run_check((1,)), [])

    def test_alters_when_the_column_is_missing(self):
        self.assertEqual(len(self.run_check(None)), 1)


if __name__ == "__main__":
    unittest.main()
