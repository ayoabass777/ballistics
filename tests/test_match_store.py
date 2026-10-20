"""Match page store (engine.eval.match_store)."""
import json
import unittest

from engine.eval import match_store as ms
from tests.test_match_detail import EXPORT_RAW


class FakeConn:
    def __init__(self):
        self.sql, self.committed, self.closed = [], False, False
    def __enter__(self):
        return self
    def __exit__(self, exc_type, *a):
        self.committed = exc_type is None
        return False
    def cursor(self):
        conn = self
        class C:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def execute(self, sql, params=None): conn.sql.append((sql, params))
            def fetchone(self): return None
            mogrify = None
        return C()
    def close(self):
        self.closed = True


class StoreTests(unittest.TestCase):
    def test_build_rows_page_ready(self):
        rows = ms.build_rows(EXPORT_RAW)
        self.assertEqual([fid for fid, _ in rows], [90876])
        sec = rows[0][1]
        self.assertEqual(set(sec), {"strength", "patterns", "goals", "form", "discover", "discovery"})
        self.assertNotIn("alpha", json.dumps(sec))

    def test_write_is_one_transaction_and_marks_complete(self):
        conn = FakeConn()
        inserted = []
        orig = ms.execute_values
        ms.execute_values = lambda cur, sql, rows: inserted.extend(rows)
        try:
            n = ms.write_match_analysis("2026-10-13", dict(EXPORT_RAW, generated="2026-10-14 07:05 UTC"), conn=conn)
        finally:
            ms.execute_values = orig
        self.assertEqual(n, 1)
        self.assertTrue(conn.committed)
        self.assertFalse(conn.closed)                      # caller owns the connection
        statements = " ".join(sql for sql, _ in conn.sql)
        self.assertIn("DELETE FROM raw.match_analysis", statements)
        self.assertIn("'complete'", statements)
        self.assertLess(statements.index("DELETE FROM raw.match_analysis"), statements.index("INSERT INTO raw.engine_runs"))
        self.assertEqual(inserted[0][:2], ("2026-10-13", 90876))
        complete_params = conn.sql[-1][1]
        self.assertEqual(complete_params[1], 1)
        self.assertEqual(complete_params[2].isoformat(), "2026-10-14T07:05:00+00:00")

    def test_generated_parse_tolerates_missing(self):
        self.assertIsNone(ms._parse_generated({}))
        self.assertIsNone(ms._parse_generated({"generated": "garbage"}))


if __name__ == "__main__":
    unittest.main()
