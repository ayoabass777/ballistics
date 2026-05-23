from __future__ import annotations

import unittest
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

try:
    import psycopg2  # noqa: F401
except ModuleNotFoundError:
    psycopg2_stub = types.ModuleType("psycopg2")
    psycopg2_stub.Error = Exception

    extras_stub = types.ModuleType("psycopg2.extras")
    extras_stub.Json = lambda value: value

    class SqlString(str):
        def format(self, *args, **kwargs):
            return SqlString(str(self).format(*args, **kwargs))

        def join(self, iterable):
            return SqlString(str(self).join(str(item) for item in iterable))

    sql_stub = types.ModuleType("psycopg2.sql")
    sql_stub.SQL = lambda value: SqlString(value)
    sql_stub.Identifier = lambda value: SqlString(value)

    psycopg2_stub.extras = extras_stub
    psycopg2_stub.sql = sql_stub

    sys.modules["psycopg2"] = psycopg2_stub
    sys.modules["psycopg2.extras"] = extras_stub
    sys.modules["psycopg2.sql"] = sql_stub

extract_metadata_stub = types.ModuleType("etl.src.extract_metadata")
extract_metadata_stub.get_db_connection = lambda: None
sys.modules["etl.src.extract_metadata"] = extract_metadata_stub

from etl.src.data_detector import data_movements, pipeline_runs, quality_events, table_snapshots
from etl.src.data_detector.watermarks import fixture_kickoff_watermark


class FakeCursor:
    def __init__(self, fetchone_values=None, fetchall_values=None):
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])
        self.executions = []

    def execute(self, query, params=None):
        self.executions.append((str(query), params))

    def fetchone(self):
        if not self.fetchone_values:
            raise AssertionError("Unexpected fetchone call")
        return self.fetchone_values.pop(0)

    def fetchall(self):
        if not self.fetchall_values:
            raise AssertionError("Unexpected fetchall call")
        return self.fetchall_values.pop(0)


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class FakeCursorContext(FakeCursor):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def airflow_context():
    return {
        "dag": SimpleNamespace(dag_id="fixture_update_dag"),
        "task": SimpleNamespace(task_id="load_updates"),
        "dag_run": SimpleNamespace(run_id="scheduled__2026-05-21", run_type=SimpleNamespace(value="scheduled")),
        "task_instance": SimpleNamespace(try_number=2, map_index=-1),
        "logical_date": datetime(2026, 5, 21, tzinfo=timezone.utc),
        "data_interval_start": datetime(2026, 5, 20, tzinfo=timezone.utc),
        "data_interval_end": datetime(2026, 5, 21, tzinfo=timezone.utc),
    }


class PipelineRunDetectorTests(unittest.TestCase):
    def test_started_event_uses_airflow_identity_and_producer_time(self):
        event_time = datetime(2026, 5, 21, 7, 1, 13, tzinfo=timezone.utc)

        with (
            patch.object(pipeline_runs, "utc_now", return_value=event_time),
            patch.object(pipeline_runs, "record_pipeline_run") as record,
        ):
            pipeline_runs.pipeline_run_started(airflow_context())

        record.assert_called_once()
        payload = record.call_args.kwargs
        self.assertEqual(payload["run_id"], "scheduled__2026-05-21")
        self.assertEqual(payload["dag_id"], "fixture_update_dag")
        self.assertEqual(payload["task_id"], "load_updates")
        self.assertEqual(payload["run_type"], "scheduled")
        self.assertEqual(payload["status"], "running")
        self.assertEqual(payload["started_at"], event_time)
        self.assertIsNone(payload["finished_at"])
        self.assertEqual(payload["metadata"]["try_number"], 2)
        self.assertEqual(payload["metadata"]["data_interval_start"], "2026-05-20T00:00:00+00:00")

    def test_failed_event_records_error_message_and_finished_at(self):
        event_time = datetime(2026, 5, 21, 7, 5, tzinfo=timezone.utc)
        context = airflow_context()
        context["exception"] = RuntimeError("load failed")

        with (
            patch.object(pipeline_runs, "utc_now", return_value=event_time),
            patch.object(pipeline_runs, "record_pipeline_run") as record,
        ):
            pipeline_runs.pipeline_run_failed(context)

        payload = record.call_args.kwargs
        self.assertEqual(payload["status"], "failed")
        self.assertIsNone(payload["started_at"])
        self.assertEqual(payload["finished_at"], event_time)
        self.assertEqual(payload["error_message"], "load failed")

    def test_skipped_event_records_skipped_status_and_finished_at(self):
        event_time = datetime(2026, 5, 21, 7, 6, tzinfo=timezone.utc)

        with (
            patch.object(pipeline_runs, "utc_now", return_value=event_time),
            patch.object(pipeline_runs, "record_pipeline_run") as record,
        ):
            pipeline_runs.pipeline_run_skipped(airflow_context())

        payload = record.call_args.kwargs
        self.assertEqual(payload["status"], "skipped")
        self.assertIsNone(payload["started_at"])
        self.assertEqual(payload["finished_at"], event_time)

    def test_success_callback_preserves_skipped_task_instance_state(self):
        event_time = datetime(2026, 5, 21, 7, 7, tzinfo=timezone.utc)
        context = airflow_context()
        context["task_instance"].state = "skipped"

        with (
            patch.object(pipeline_runs, "utc_now", return_value=event_time),
            patch.object(pipeline_runs, "record_pipeline_run") as record,
        ):
            pipeline_runs.pipeline_run_succeeded(context)

        payload = record.call_args.kwargs
        self.assertEqual(payload["status"], "skipped")
        self.assertEqual(payload["finished_at"], event_time)


class DataMovementDetectorTests(unittest.TestCase):
    def test_context_wrapper_emits_movement_contract_fields(self):
        watermark_min = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
        watermark_max = datetime(2026, 5, 21, 20, tzinfo=timezone.utc)

        with patch.object(data_movements, "record_data_movement") as record:
            data_movements.record_data_movement_from_context(
                airflow_context(),
                movement_type="s3_to_raw",
                source_system="s3",
                status="success",
                source_s3_key="incremental/2026-05-21/fixtures.json",
                target_schema="raw",
                target_table="raw_fixtures",
                row_count=43,
                updated_count=43,
                watermark_column="kickoff_utc",
                watermark_min=watermark_min,
                watermark_max=watermark_max,
                details={"source": "unit-test"},
            )

        payload = record.call_args.kwargs
        self.assertEqual(payload["run_id"], "scheduled__2026-05-21")
        self.assertEqual(payload["dag_id"], "fixture_update_dag")
        self.assertEqual(payload["task_id"], "load_updates")
        self.assertEqual(payload["movement_type"], "s3_to_raw")
        self.assertEqual(payload["source_system"], "s3")
        self.assertEqual(payload["source_s3_key"], "incremental/2026-05-21/fixtures.json")
        self.assertEqual(payload["target_schema"], "raw")
        self.assertEqual(payload["target_table"], "raw_fixtures")
        self.assertEqual(payload["row_count"], 43)
        self.assertEqual(payload["updated_count"], 43)
        self.assertEqual(payload["failed_count"], 0)
        self.assertEqual(payload["watermark_column"], "kickoff_utc")
        self.assertEqual(payload["watermark_min"], watermark_min)
        self.assertEqual(payload["watermark_max"], watermark_max)
        self.assertEqual(payload["status"], "success")

    def test_fixture_watermark_uses_kickoff_coverage(self):
        fixtures = [
            {"kickoff_utc": "2026-05-21T18:00:00Z"},
            {"kickoff_utc": "2026-05-20T12:00:00+00:00"},
            {"kickoff_utc": None},
            {"kickoff_utc": "not-a-date"},
        ]

        column, min_value, max_value = fixture_kickoff_watermark(fixtures)

        self.assertEqual(column, "kickoff_utc")
        self.assertEqual(min_value, datetime(2026, 5, 20, 12, tzinfo=timezone.utc))
        self.assertEqual(max_value, datetime(2026, 5, 21, 18, tzinfo=timezone.utc))


class TableSnapshotDetectorTests(unittest.TestCase):
    def test_snapshot_reads_available_columns_and_inserts_raw_fixture_metrics(self):
        cursor = FakeCursorContext(
            fetchall_values=[[("created_at",), ("updated_at",), ("kickoff_utc",)]],
            fetchone_values=[(156420, "created-max", "updated-max", "kickoff-max")],
        )
        connection = FakeConnection(cursor)

        with patch.object(table_snapshots, "get_db_connection", return_value=connection):
            table_snapshots.record_table_snapshot(
                table_schema="raw",
                table_name="raw_fixtures",
                run_id="scheduled__2026-05-21",
                dag_id="fixture_update_dag",
                details={"stage": "unit-test"},
            )

        self.assertEqual(connection.commits, 1)
        self.assertIn("information_schema.columns", cursor.executions[0][0])
        self.assertIn("COUNT(*)::bigint AS row_count", cursor.executions[1][0])
        insert_query, insert_params = cursor.executions[2]
        self.assertIn("INSERT INTO data_detector.table_snapshots", insert_query)
        self.assertEqual(insert_params[1], "scheduled__2026-05-21")
        self.assertEqual(insert_params[2], "fixture_update_dag")
        self.assertEqual(insert_params[3], "raw")
        self.assertEqual(insert_params[4], "raw_fixtures")
        self.assertEqual(insert_params[5], 156420)
        self.assertEqual(insert_params[8], "kickoff-max")

    def test_snapshot_fails_clearly_for_missing_table(self):
        cursor = FakeCursorContext(fetchall_values=[[]])
        connection = FakeConnection(cursor)

        with patch.object(table_snapshots, "get_db_connection", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "Missing table raw.missing_table"):
                table_snapshots.record_table_snapshot(table_schema="raw", table_name="missing_table")


class QualityDetectorTests(unittest.TestCase):
    def test_raw_fixture_stale_requires_current_seasons_unresolved_past_and_stale_table(self):
        cursor = FakeCursorContext(
            fetchone_values=[
                (1,),
                (3,),
                (datetime(2026, 5, 19, tzinfo=timezone.utc), "2 days"),
                (True,),
            ]
        )
        connection = FakeConnection(cursor)

        with (
            patch.object(quality_events, "get_db_connection", return_value=connection),
            patch.object(quality_events, "record_data_quality_event") as record,
        ):
            quality_events.check_raw_fixture_stale_from_context(airflow_context())

        record.assert_called_once()
        payload = record.call_args.kwargs
        self.assertEqual(payload["severity"], "warning")
        self.assertEqual(payload["check_name"], "raw_fixture_stale")
        self.assertEqual(payload["table_schema"], "raw")
        self.assertEqual(payload["table_name"], "raw_fixtures")
        self.assertEqual(payload["details"]["stale_hours"], quality_events.RAW_FIXTURE_STALE_HOURS)
        self.assertEqual(payload["details"]["current_season_count"], 1)
        self.assertEqual(payload["details"]["unresolved_past_fixture_count"], 3)

    def test_raw_fixture_stale_does_not_warn_without_unresolved_past_fixtures(self):
        cursor = FakeCursorContext(
            fetchone_values=[
                (1,),
                (0,),
                (datetime(2026, 5, 19, tzinfo=timezone.utc), "2 days"),
                (True,),
            ]
        )
        connection = FakeConnection(cursor)

        with (
            patch.object(quality_events, "get_db_connection", return_value=connection),
            patch.object(quality_events, "record_data_quality_event") as record,
        ):
            quality_events.check_raw_fixture_stale_from_context(airflow_context())

        record.assert_not_called()

    def test_standings_stale_records_warning_when_updated_at_is_old(self):
        cursor = FakeCursorContext(
            fetchall_values=[
                [
                    (
                        101,
                        39,
                        2025,
                        datetime(2026, 5, 19, tzinfo=timezone.utc),
                        "2 days",
                    )
                ],
            ]
        )
        connection = FakeConnection(cursor)

        with (
            patch.object(quality_events, "get_db_connection", return_value=connection),
            patch.object(quality_events, "record_data_quality_event") as record,
        ):
            quality_events.check_standings_stale_from_context(airflow_context(), stale_days=7)

        record.assert_called_once()
        payload = record.call_args.kwargs
        self.assertEqual(payload["check_name"], "standings_stale")
        self.assertEqual(payload["table_schema"], "raw")
        self.assertEqual(payload["table_name"], "raw_league_standings")
        self.assertEqual(payload["details"]["stale_days"], 7)
        self.assertEqual(payload["details"]["stale_league_count"], 1)
        self.assertEqual(
            payload["details"]["stale_leagues"],
            [
                {
                    "league_season_id": 101,
                    "api_league_id": 39,
                    "season": 2025,
                    "max_updated_at": "2026-05-19T00:00:00+00:00",
                    "update_age": "2 days",
                }
            ],
        )

    def test_standings_stale_does_not_warn_when_no_current_leagues_are_stale(self):
        cursor = FakeCursorContext(fetchall_values=[[]])
        connection = FakeConnection(cursor)

        with (
            patch.object(quality_events, "get_db_connection", return_value=connection),
            patch.object(quality_events, "record_data_quality_event") as record,
        ):
            quality_events.check_standings_stale_from_context(airflow_context(), stale_days=7)

        record.assert_not_called()


class DagWiringTests(unittest.TestCase):
    def test_bootstrap_detector_helpers_are_scoped_to_correct_tasks(self):
        source = Path("airflow/dags/bootstrap_dag.py").read_text()
        extract_task_start = source.index("    def extract_fixtures")
        load_task_start = source.index("    def load_fixtures")
        extract_task_source = source[extract_task_start:load_task_start]
        load_task_source = source[load_task_start:]

        self.assertIn("record_data_movement_from_context", extract_task_source)
        self.assertNotIn("record_table_snapshot_from_context", extract_task_source)
        self.assertNotIn("record_data_quality_event_from_context", extract_task_source)

        self.assertIn("record_table_snapshot_from_context", load_task_source)
        self.assertIn("record_data_quality_event_from_context", load_task_source)
        self.assertIn("from etl.src.data_detector import (", load_task_source)

    def test_fixture_update_detector_helpers_are_scoped_to_correct_tasks(self):
        source = Path("airflow/dags/fixture_update.py").read_text()
        extract_task_start = source.index("    def extract_updates")
        load_task_start = source.index("    def load_updates")
        extract_task_source = source[extract_task_start:load_task_start]
        load_task_source = source[load_task_start:]

        self.assertIn("record_data_movement_from_context", extract_task_source)
        self.assertNotIn("record_table_snapshot_from_context", extract_task_source)
        self.assertNotIn("record_data_quality_event_from_context", extract_task_source)

        self.assertIn("record_table_snapshot_from_context", load_task_source)
        self.assertIn("from etl.src.data_detector import (", load_task_source)

        correction_task_start = source.index("    def fixture_corrections")
        standings_task_start = source.index("    def update_standings_task")
        correction_task_source = source[correction_task_start:standings_task_start]

        self.assertIn('movement_type="raw_correction"', correction_task_source)
        self.assertIn("count_of_corrected_fixtures", correction_task_source)
        self.assertNotIn("fixture_bound", correction_task_source)
        self.assertNotIn("count=11", correction_task_source)
        self.assertIn("record_data_movement_from_context", correction_task_source)
        self.assertNotIn("record_table_snapshot_from_context", correction_task_source)
        self.assertNotIn("record_data_quality_event_from_context", correction_task_source)

        standings_task_start = source.index("    def update_standings_task")
        quality_task_start = source.index("    @task\n    def run_data_quality_checks")
        standings_task_source = source[standings_task_start:quality_task_start]

        self.assertIn("return_summary=True", standings_task_source)
        self.assertIn('"leagues_updated": standings_summary["leagues_updated"]', standings_task_source)
        self.assertIn("check_standings_stale_from_context", standings_task_source)


if __name__ == "__main__":
    unittest.main()
