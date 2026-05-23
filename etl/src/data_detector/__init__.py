"""
Data detector package facade.
"""
from etl.src.data_detector.data_movements import record_data_movement, record_data_movement_from_context
from etl.src.data_detector.pipeline_runs import (
    pipeline_run_failed,
    pipeline_run_skipped,
    pipeline_run_started,
    pipeline_run_succeeded,
    record_pipeline_run,
)
from etl.src.data_detector.quality_events import (
    check_raw_fixture_stale_from_context,
    check_standings_stale_from_context,
    record_data_quality_event,
    record_data_quality_event_from_context,
)
from etl.src.data_detector.table_snapshots import record_table_snapshot, record_table_snapshot_from_context
from etl.src.data_detector.watermarks import fixture_kickoff_watermark

__all__ = [
    "check_raw_fixture_stale_from_context",
    "check_standings_stale_from_context",
    "fixture_kickoff_watermark",
    "pipeline_run_failed",
    "pipeline_run_skipped",
    "pipeline_run_started",
    "pipeline_run_succeeded",
    "record_data_movement",
    "record_data_movement_from_context",
    "record_data_quality_event",
    "record_data_quality_event_from_context",
    "record_pipeline_run",
    "record_table_snapshot",
    "record_table_snapshot_from_context",
]
