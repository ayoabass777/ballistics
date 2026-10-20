"""
@monitored: metadata, monitoring and alerting for DLQ-aware loaders (D148).

Wrap a loader that returns an etl.src.dlq.LoadResult (place it above
@dead_letter). For every call it records, best effort and never failing the
load:

  data_detector.data_movements   movement_type 'row_validation': rows in,
                                 loaded, parked, reasons, the DLQ key, the
                                 source (nightly / bootstrap / replay), timing
  data_detector.data_quality_events
                                 opened (warning, check 'unprocessed_rows')
                                 the first time a league's rows are parked;
                                 resolved when a clean load closes the entry
  alert                          sent once, when rows are first parked (or
                                 when they could not be written to the DLQ).
                                 Hourly replays that still fail stay quiet;
                                 the entry alerts again only when it goes dead
                                 (etl.src.dlq.alert_dead).

    @monitored(dataset="standings", target_table="raw.raw_league_standings")
    @dead_letter(validate=..., park=...)
    def load_league_standings(rows, *, league, ds, source, ...): ...
"""
from __future__ import annotations

import functools
import time
from typing import Any, Dict, Optional

from etl.src.data_detector.common import logger

CHECK_NAME = "unprocessed_rows"


def _airflow_identity() -> Dict[str, Optional[str]]:
    """run/dag/task ids when called inside an Airflow task, else Nones."""
    try:
        from airflow.operators.python import get_current_context
        from etl.src.data_detector.common import airflow_identity
        return airflow_identity(get_current_context())
    except Exception:
        return {"run_id": None, "dag_id": None, "task_id": None}


def _label(row: Dict[str, Any]) -> str:
    """Club Leon / Toluca v America / fixture 123"""
    if row.get("team_name"):
        return str(row["team_name"])
    if row.get("home_team_name") or row.get("away_team_name"):
        return f"{row.get('home_team_name')} v {row.get('away_team_name')}"
    return f"fixture {row.get('api_fixture_id')}"


def describe(dataset: str, league: Dict[str, Any], result) -> Dict[str, Any]:
    """The metadata block recorded for one load."""
    from etl.src.dlq import DLQ_REASON
    return {
        "dataset": dataset,
        "api_league_id": league.get("api_league_id"),
        "season": league.get("season"),
        "league_season_id": league.get("league_season_id"),
        "rows_in": result.loaded + len(result.parked),
        "loaded": result.loaded,
        "parked": len(result.parked),
        "reasons": sorted({r.get(DLQ_REASON) for r in result.parked if r.get(DLQ_REASON)}),
        "parked_rows": [_label(r) for r in result.parked][:20],
        "dlq_key": result.dlq_key,
        "new_entry": result.new_entry,
        "resolved": result.resolved,
        "dlq_write_failed": result.dlq_write_failed,
    }


def _alert(dataset: str, meta: Dict[str, Any]) -> None:
    from etl.src.data_detector.notify import send_alert
    where = f"league {meta['api_league_id']} season {meta['season']}"
    if meta["dlq_write_failed"]:
        subject = f"✗ Ballistics {dataset}: {meta['parked']} unprocessable row(s), DLQ write failed"
        tail = "The rows could not be written to the S3 DLQ, so they will not be retried automatically."
    else:
        subject = f"! Ballistics {dataset}: {meta['parked']} unprocessable row(s) parked"
        tail = (f"Parked at {meta['dlq_key']}; retried hourly, given up after 24 attempts. "
                "The rest of the league loaded.")
    body = "\n".join([
        f"{dataset.capitalize()} for {where}: {meta['parked']} of {meta['rows_in']} rows could not be loaded.",
        f"Reason: {', '.join(meta['reasons']) or 'unknown'}",
        f"Rows: {', '.join(meta['parked_rows'])}",
        tail,
    ])
    send_alert(subject, body)


def record(dataset: str, target_table: str, league: Dict[str, Any], result, *,
           source: str, duration_ms: int) -> Dict[str, Any]:
    """Write the movement record, open/resolve the quality event, alert."""
    from etl.src.data_detector.data_movements import record_data_movement
    from etl.src.data_detector.quality_events import (
        record_data_quality_event,
        resolve_data_quality_events,
    )
    meta = {**describe(dataset, league, result), "source": source, "duration_ms": duration_ms}
    ids = _airflow_identity()
    schema, _, table = target_table.partition(".")
    key = f"{dataset}:{league.get('api_league_id')}:{league.get('season')}"
    steps = []
    steps.append(lambda: record_data_movement(
        **ids, movement_type="row_validation", source_system="validation", source_name=key,
        target_schema=schema, target_table=table, row_count=meta["rows_in"],
        inserted_count=result.loaded, failed_count=len(result.parked),
        status="partial" if result.parked else "success", details=meta))
    if result.parked and (result.new_entry or result.dlq_write_failed):
        steps.append(lambda: record_data_quality_event(
            **ids, severity="error" if result.dlq_write_failed else "warning",
            check_name=CHECK_NAME, status="open", table_schema=schema, table_name=table,
            source_s3_key=result.dlq_key, details=meta,
            error_message=f"{meta['parked']} unprocessable {dataset} row(s) for {key}"))
        steps.append(lambda: _alert(dataset, meta))
    if result.resolved:
        steps.append(lambda: resolve_data_quality_events(
            check_name=CHECK_NAME, table_schema=schema, table_name=table,
            reason=f"clean {source} load for {key}"))
    for step in steps:
        try:
            step()
        except Exception as exc:     # monitoring never fails the load
            logger.error("monitoring for %s failed: %s", key, exc)
    return meta


def monitored(*, dataset: str, target_table: str):
    def decorate(loader):
        @functools.wraps(loader)
        def wrapper(rows, *args, **kwargs):
            start = time.monotonic()
            result = loader(rows, *args, **kwargs)
            record(dataset, target_table, kwargs.get("league") or {}, result,
                   source=kwargs.get("source", "nightly"),
                   duration_ms=int((time.monotonic() - start) * 1000))
            return result
        wrapper.monitored_dataset = dataset
        return wrapper
    return decorate
