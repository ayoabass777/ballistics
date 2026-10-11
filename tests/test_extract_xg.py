from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from etl.src import extract_xg


class CursorContext:
    def __init__(self, rows=None, description=None):
        self.rows = list(rows or [])
        self.description = description or []
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.executions.append((str(query), params))

    def fetchall(self):
        return list(self.rows)


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.commits = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


class ExtractXgTests(unittest.TestCase):
    def test_eredivisie_is_enabled_for_scheduled_ingestion(self):
        self.assertIn(88, extract_xg.XG_LEAGUES)

    def test_fetch_xg_extracts_expected_goals_by_team(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "errors": [],
            "response": [
                {
                    "team": {"id": 197},
                    "statistics": [
                        {"type": "Shots on Goal", "value": 9},
                        {"type": "expected_goals", "value": "1.73"},
                    ],
                },
                {
                    "team": {"id": 426},
                    "statistics": [
                        {"type": "Expected_Goals", "value": 1.21},
                    ],
                },
            ],
        }

        with patch.object(extract_xg.requests, "get", return_value=response):
            result = extract_xg._fetch_xg(1380871)

        self.assertEqual(result, {197: 1.73, 426: 1.21})

    def test_bootstrap_creates_xg_table_probe_table_and_coverage_view(self):
        cursor = CursorContext()
        conn = FakeConnection(cursor)

        extract_xg.bootstrap(conn)

        self.assertEqual(conn.commits, 1)
        statements = "\n".join(query for query, _ in cursor.executions)
        self.assertIn("raw.fixture_xg", statements)
        self.assertIn("raw.xg_league_probes", statements)
        self.assertIn("raw.fixture_xg_attempts", statements)
        self.assertIn("raw.xg_league_coverage", statements)

    def test_probe_records_provider_support_without_requiring_allowlist(self):
        cursor = CursorContext(rows=[
            (101, 1001, 88, 197, 42),
            (102, 1002, 88, 197, 43),
            (201, 2001, 999, 50, 51),
            (202, 2002, 999, 52, 53),
        ])
        conn = FakeConnection(cursor)
        provider_values = {
            1001: {197: 1.8, 42: 0.7},
            1002: {197: 2.1, 43: 0.9},
            2001: {},
            2002: {},
        }

        with (
            patch.object(extract_xg, "get_db_connection", return_value=conn),
            patch.object(extract_xg, "bootstrap"),
            patch.object(extract_xg, "_fetch_xg",
                         side_effect=lambda api_fid: provider_values[api_fid]),
            patch.object(extract_xg.time, "sleep"),
        ):
            result = extract_xg.probe_league_coverage(
                seasons=[2025], sample_size=2, store_available=False,
            )

        by_league = {row["api_league_id"]: row for row in result}
        self.assertTrue(by_league[88]["provider_supported"])
        self.assertEqual(by_league[88]["sample_coverage_pct"], 100.0)
        self.assertFalse(by_league[999]["provider_supported"])
        self.assertEqual(by_league[999]["sample_coverage_pct"], 0.0)
        self.assertEqual(conn.commits, 2)
        self.assertTrue(conn.closed)
        self.assertTrue(any("INSERT INTO raw.xg_league_probes" in query
                            for query, _ in cursor.executions))

    def test_coverage_report_marks_configured_leagues(self):
        columns = [
            "api_league_id", "league_name", "season", "completed_fixtures",
            "complete_xg_fixtures", "coverage_pct", "last_completed_kickoff",
            "last_xg_captured_at", "missing_xg_attempts", "error_xg_attempts",
            "last_xg_attempt_at", "provider_supported", "configured",
        ]
        cursor = CursorContext(
            rows=[(88, "Eredivisie", 2025, 306, 1, 0.33, None, None,
                   5, 0, None, True, True)],
            description=[(name,) for name in columns],
        )
        conn = FakeConnection(cursor)

        with (
            patch.object(extract_xg, "get_db_connection", return_value=conn),
            patch.object(extract_xg, "bootstrap"),
        ):
            rows = extract_xg.coverage_report(seasons=[2025], leagues=[88])

        self.assertEqual(rows[0]["league_name"], "Eredivisie")
        self.assertTrue(rows[0]["configured"])
        query, params = cursor.executions[0]
        self.assertIn("raw.xg_league_coverage", query)
        self.assertEqual(params[1], [2025])
        self.assertEqual(params[2], [88])

    def test_attempt_status_is_validated_before_insert(self):
        cursor = CursorContext()

        with self.assertRaisesRegex(ValueError, "Unsupported xG attempt status"):
            extract_xg._record_attempt(cursor, 1, 2, "unknown")

        self.assertEqual(cursor.executions, [])


if __name__ == "__main__":
    unittest.main()
