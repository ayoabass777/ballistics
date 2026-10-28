"""Daily flow check: did data flow through every dataset in the chain for a day?

For one Airflow logical date (ds), each dataset in the registry (datasets.yml)
is checked with its own rule and classified:

  ok       arrived for ds
  failed   the producer ran and failed
  missing  nothing recorded for ds
  blocked  not ok because something upstream is not ok (blamed on that root)

Results are kept per day in data_detector.daily_flow_checks (history: did the
chain complete on each day). Each root failure opens one ``daily_flow``
quality event; datasets that recover resolve theirs. An alert goes out when
there are root failures, and a recovery note when earlier failures clear.

CLI:  python -m etl.src.data_detector.daily_flow 2026-10-13 [--no-notify]
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import date, timedelta
from typing import Any

from etl.src.data_detector.common import logger
from etl.src.data_detector.datasets import Registry, effective_severity, load_registry, sync_registry

CHECK_NAME = "daily_flow"
NOT_OK = {"failed", "missing", "blocked"}
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$")

DDL = """
CREATE TABLE IF NOT EXISTS data_detector.daily_flow_checks (
  check_date   DATE        NOT NULL,
  dataset_id   TEXT        NOT NULL,
  layer        TEXT        NOT NULL,
  status       TEXT        NOT NULL,       -- ok | failed | missing | blocked
  blocked_by   TEXT[]      NOT NULL DEFAULT '{}',
  details      JSONB       NOT NULL DEFAULT '{}'::jsonb,
  checked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (check_date, dataset_id)
);
"""

# pipeline_runs stores the Airflow logical date in metadata (pipeline_runs.py),
# compared in UTC so the session time zone cannot shift the day.
PIPELINE_SQL = """
SELECT status, finished_at, error_message
FROM data_detector.pipeline_runs
WHERE dag_id = %s AND task_id = %s AND ((metadata->>'logical_date')::timestamptz AT TIME ZONE 'UTC')::date = %s
ORDER BY updated_at DESC, pipeline_run_id DESC
LIMIT 1
"""

MOVEMENT_SQL = """
SELECT status, row_count
FROM data_detector.data_movements
WHERE dag_id = %s AND movement_type = %s
  AND run_id IN (SELECT run_id FROM data_detector.pipeline_runs
                 WHERE dag_id = %s
                   AND ((metadata->>'logical_date')::timestamptz AT TIME ZONE 'UTC')::date = %s)
ORDER BY data_movement_id DESC
"""


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def _ident(name: str) -> str:
    if not _IDENT.match(name or ""):
        raise ValueError(f"unsafe identifier in registry: {name!r}")
    return name


def check_pipeline_task(cur, spec: dict, ds: date, task_state=None) -> dict:
    """Task state from data_detector.pipeline_runs; when the detector has no
    record (callback missed or not yet deployed), ask Airflow via task_state."""
    cur.execute(PIPELINE_SQL, (spec["dag"], spec["task"], ds))
    row = cur.fetchone()
    source = {}
    if row is None and task_state is not None:
        state = task_state(spec["dag"], spec["task"], ds)
        if state:
            row, source = (state, None, None), {"source": "airflow (no detector record)"}
    if row is None:
        return {"status": "missing", "details": {"reason": "task did not run for this day"}}
    status, finished_at, error = row
    if status == "success":
        return {"status": "ok", "details": {**({"finished_at": str(finished_at)} if finished_at else {}), **source}}
    if status == "skipped":
        return {"status": "ok", "details": {"note": "task skipped by the DAG", **source}}
    if status in ("failed", "upstream_failed"):
        return {"status": "failed", "details": {"error": (error or "")[:300], **source}}
    return {"status": "missing", "details": {"reason": f"task state {status}", **source}}


def check_movement(cur, spec: dict, ds: date) -> dict:
    cur.execute(MOVEMENT_SQL, (spec["dag"], spec["movement_type"], spec["dag"], ds))
    rows = cur.fetchall()
    if not rows:
        return {"status": "missing", "details": {"reason": f"no {spec['movement_type']} movement"}}
    good = [r for r in rows if r[0] == "success"]
    if good:
        return {"status": "ok", "details": {"rows": sum(r[1] or 0 for r in good)}}
    return {"status": "failed", "details": {"movements": len(rows)}}


def _producer_tasks(spec: dict) -> list[str]:
    if spec.get("tasks"):
        return list(spec["tasks"])
    return [spec["task"]] if spec.get("task") else []


def _explain_shortfall(cur, spec: dict, ds: date, details: dict) -> dict:
    """Rows are short: if a producing task failed, say so (failed beats missing)."""
    tasks = _producer_tasks(spec)
    if not tasks or not spec.get("dag"):
        return {"status": "missing", "details": details}
    failed, states = [], {}
    for task_id in tasks:
        cur.execute(PIPELINE_SQL, (spec["dag"], task_id, ds))
        row = cur.fetchone()
        states[task_id] = row[0] if row else "not run"
        if row and row[0] in ("failed", "upstream_failed"):
            failed.append({"task": task_id, "error": (row[2] or "")[:300]})
    details = {**details, "tasks": states}
    if failed:
        return {"status": "failed", "details": {**details, "failed_tasks": failed}}
    return {"status": "missing", "details": details}


def check_run_ds_rows(cur, spec: dict, ds: date) -> dict:
    table, column = _ident(spec["table"]), _ident(spec["column"])
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (table,))
    if not cur.fetchone()[0]:
        return _explain_shortfall(cur, spec, ds, {"reason": f"{table} does not exist"})
    where = f" AND ({spec['where']})" if spec.get("where") else ""
    cur.execute(f"SELECT count(*) FROM {table} WHERE {column} = %s{where}", (ds,))
    n = cur.fetchone()[0]
    need = int(spec.get("min_rows", 1))
    if n >= need:
        return {"status": "ok", "details": {"rows": n}}
    return _explain_shortfall(cur, spec, ds, {"rows": n, "min_rows": need})


def check_sql_assert(cur, spec: dict, ds: date) -> dict:
    """Registry SQL returns (bad_count, sample); ok when bad_count <= max_bad.

    For current-state checks (e.g. crest coverage) that do not depend on ds.
    The SQL comes from the reviewed registry file, never from user input.
    """
    cur.execute(spec["sql"])
    bad, sample = cur.fetchone()
    bad = int(bad or 0)
    details = {"bad": bad}
    if sample:
        details["sample"] = sample
    if bad <= int(spec.get("max_bad", 0)):
        return {"status": "ok", "details": details}
    return {"status": "failed", "details": details}


def crest_base_url() -> str:
    import os
    explicit = os.getenv("CREST_BASE_URL")
    if explicit:
        return explicit.rstrip("/")
    bucket = os.getenv("ASSETS_BUCKET", "ballistics-assets")
    region = os.getenv("AWS_DEFAULT_REGION", "ca-central-1")
    return f"https://{bucket}.s3.{region}.amazonaws.com"


# Waits (seconds) before re-sampling crests when no URL answered at all.
CREST_NETWORK_RETRY_WAITS = (30, 90)


def _sample_crests(base: str, rows) -> tuple[list[str], list[str]]:
    """HEAD each crest. Returns (broken, unreachable): broken = the URL answered
    wrongly (crest problem); unreachable = no answer at all (network problem)."""
    import requests
    broken, unreachable = [], []
    for kind, file_id in rows:
        url = f"{base}/crests/{kind}/{file_id}"
        try:
            r = requests.head(url, timeout=10)
            ctype = r.headers.get("Content-Type", "")
            if r.status_code != 200 or not ctype.startswith("image/"):
                broken.append(f"{kind}/{file_id}: {r.status_code} {ctype}".strip())
        except (requests.ConnectionError, requests.Timeout) as exc:
            unreachable.append(f"{kind}/{file_id}: {type(exc).__name__}: {exc}"[:200])
        except requests.RequestException as exc:
            broken.append(f"{kind}/{file_id}: {type(exc).__name__}")
    return broken, unreachable


def check_crest_urls(cur, spec: dict, ds: date) -> dict:
    """HEAD a sample of public crest URLs: each must answer 200 with an image.

    If no URL answers at all (DNS failure, no route), that is the network, not
    the crests: re-sample after a wait, and if it is still down report it as a
    warning ("network down, crests not checked") instead of a crest failure.
    """
    import time
    cur.execute(spec["sql"])                       # returns (kind, logo_file_id) rows
    rows = cur.fetchall()
    if not rows:
        return {"status": "missing", "details": {"reason": "no crests to sample"}}
    base = crest_base_url()
    details = {"sampled": len(rows), "base_url": base}
    broken, unreachable = _sample_crests(base, rows)
    for wait in CREST_NETWORK_RETRY_WAITS:
        if broken or len(unreachable) < len(rows):
            break                                  # at least one answer: the network works
        logger.warning("crest check: no URL reachable (%s); retrying in %ss", unreachable[0], wait)
        time.sleep(wait)
        broken, unreachable = _sample_crests(base, rows)
    if not broken and unreachable and len(unreachable) == len(rows):
        return {"status": "failed", "severity": "warning",
                "details": {**details, "reason": "network down: crests not checked",
                            "error": unreachable[0]}}
    broken += unreachable                          # partial outage: count with the broken
    if broken:
        return {"status": "failed", "details": {**details, "broken": broken[:5]}}
    return {"status": "ok", "details": details}


# DLQ entries younger than this are expected (the hourly replay is still on
# them): warning. Older, or given up on (dead): error. (D147)
DLQ_WARN_HOURS = 24


def _dlq_result(items: list[dict], what: str) -> dict:
    """items: {label, status, age_hours, attempts, problem?} per open DLQ entry."""
    if not items:
        return {"status": "ok", "details": {"open": 0}}
    dead = [i for i in items if i["status"] == "dead"]
    stale = [i for i in items if i["status"] != "dead" and i["age_hours"] >= DLQ_WARN_HOURS]
    broken = [i for i in items if i.get("problem")]
    oldest = max(i["age_hours"] for i in items)
    lines = [f"{i['label']}: {i['status']}, {i['age_hours']:.0f}h, {i['attempts']} retries"
             + (f" ({i['problem']})" if i.get("problem") else "")
             for i in sorted(items, key=lambda i: -i["age_hours"])[:10]]
    details = {"open": len(items) - len(dead), "dead": len(dead),
               "oldest_hours": round(oldest, 1), "entries": lines}
    if dead or stale or broken:
        return {"status": "failed", "details": details}
    return {"status": "failed", "severity": "warning",
            "details": {**details, "note": f"{what} being retried hourly"}}


def _unflagged_s3_entries(prefix: str, flagged: set) -> list[dict]:
    """Open S3 entries under prefix whose league season isn't flagged in the
    status table: the rows were parked but the flag write failed, so the page
    and the DB side of this check can't see them. Mirror of 'flag without
    dlq_key' (S3 write failed)."""
    from etl.src import dlq
    items = []
    for key in dlq.list_keys(prefix):
        if not key.endswith("/open.json"):
            continue
        lid, season = key.split("/")[-3:-1]          # .../{api_league_id}/{season}/open.json
        if not (lid.isdigit() and season.isdigit()) or (int(lid), int(season)) in flagged:
            continue
        entry = dlq.with_state(dlq.read_json(key) or {})
        items.append({"label": f"league {lid}/{season} ({key})",
                      "status": entry.get("status", "open"), "age_hours": dlq.age_hours(entry),
                      "attempts": int(entry.get("attempts") or 0),
                      "problem": "not flagged in the DB: status write failed"})
    return items


def _row_dlq_check(cur, table: str, names_col: str, prefix: str, what: str, names) -> dict:
    """Row-level DLQ (standings, fixture rows): flagged leagues from the status
    table, cross-checked against the open entries on S3 in both directions."""
    items = []
    cur.execute("SELECT to_regclass(%s)", (table,))
    if cur.fetchone()[0] is not None:            # missing table = nothing flagged yet
        cur.execute(
            f"""SELECT api_league_id, season, status, {names_col},
                       EXTRACT(EPOCH FROM now() - unprocessed_since) / 3600, replay_attempts, dlq_key
                FROM {table} WHERE status <> 'ok'""")
        items = [{"label": f"league {lid}/{season} ({names(listed)})",
                  "status": "dead" if st == "dead" else "open", "age_hours": float(age or 0),
                  "attempts": attempts or 0, "league": (lid, season),
                  "problem": None if key else "not in the S3 DLQ: write failed"}
                 for lid, season, st, listed, age, attempts, key in cur.fetchall()]
    try:
        items += _unflagged_s3_entries(prefix, {i["league"] for i in items})
    except Exception as exc:                      # S3 unreachable: report the DB side only
        logger.warning("dlq check: S3 cross-check for %s skipped: %s", prefix, exc)
        result = _dlq_result(items, what)
        result["details"]["s3_cross_check"] = f"skipped: {type(exc).__name__}"[:200]
        return result
    return _dlq_result(items, what)


def check_dlq(cur, spec: dict, ds: date) -> dict:
    """Open DLQ entries for `source` (standings | fixtures).

    standings  raw.raw_league_standings_status (written with each parked row,
               so it is readable without S3); a flagged league without a
               dlq_key means the S3 write failed, and an open S3 entry
               without a flag means the status write failed: both errors.
    fixture_rows  raw.raw_fixtures_status, the same for single fixture rows (D148).
    fixtures   the error.json entries under s3://<bucket>/dlq/.
    """
    from etl.src import dlq
    source = spec.get("source")
    if source == "standings":
        from etl.src.standings_dlq import PREFIX, STATUS_TABLE
        return _row_dlq_check(cur, STATUS_TABLE, "unprocessed_teams", PREFIX, "standings rows",
                              lambda names: ", ".join(names or []))
    if source == "fixture_rows":
        from etl.src.fixtures_dlq import PREFIX, STATUS_TABLE
        return _row_dlq_check(cur, STATUS_TABLE, "unprocessed_fixtures", PREFIX, "fixture rows",
                              lambda names: ", ".join((names or [])[:3]))
    if source == "fixtures":
        from etl.src.s3_landing import list_dlq_entries
        try:
            entries = list_dlq_entries()
            bodies = [dlq.with_state(dlq.read_json(e["error_key"]) or {}) for e in entries]
        except Exception as exc:
            if "connect" in type(exc).__name__.lower() or isinstance(exc, OSError):
                return {"status": "failed", "severity": "warning",
                        "details": {"reason": "network down: DLQ not checked",
                                    "error": f"{type(exc).__name__}: {exc}"[:200]}}
            raise
        items = [{"label": f"league {e['api_league_id']}/{e['season_year']} ({e['ds']})",
                  "status": b.get("status", "open"), "age_hours": dlq.age_hours(b),
                  "attempts": int(b.get("attempts") or 0)}
                 for e, b in zip(entries, bodies)]
        return _dlq_result(items, "fixture extractions")
    raise ValueError(f"dlq check: unknown source {source!r}")


CHECKS = {"pipeline_task": check_pipeline_task, "movement": check_movement,
          "run_ds_rows": check_run_ds_rows, "sql_assert": check_sql_assert,
          "crest_urls": check_crest_urls, "dlq": check_dlq}


def evaluate(cur, registry: Registry, ds: date, task_state=None) -> dict[str, dict]:
    """Status per dataset for ds, with blame pushed to the furthest-upstream root."""
    results: dict[str, dict] = {}
    for dataset_id in registry.topological():
        d = registry[dataset_id]
        kind = d.check["type"]
        if kind == "derived":
            res = {"status": "ok", "details": {}}
        else:
            cur.execute("SAVEPOINT flow_check")
            try:
                if kind == "pipeline_task":
                    res = check_pipeline_task(cur, d.check, ds, task_state)
                else:
                    res = CHECKS[kind](cur, d.check, ds)
                cur.execute("RELEASE SAVEPOINT flow_check")
            except Exception as exc:          # a broken check is a finding, not a crash
                cur.execute("ROLLBACK TO SAVEPOINT flow_check")
                logger.error("daily_flow check %s failed: %s", dataset_id, exc)
                res = {"status": "failed", "details": {"check_error": f"{type(exc).__name__}: {exc}"[:300]}}
        bad_up = [u for u in d.upstream if results[u]["status"] in NOT_OK]
        roots = sorted({r for u in bad_up for r in (results[u].get("blocked_by") or [u])})
        if kind == "derived" and bad_up:
            res = {"status": "blocked", "details": {}}
        elif res["status"] in NOT_OK and bad_up:
            res = {"status": "blocked", "details": {"own_check": res["status"], **res["details"]}}
        res["blocked_by"] = roots if res["status"] == "blocked" else []
        res["layer"] = d.layer
        results[dataset_id] = res
    return results


def roots(results: dict[str, dict]) -> list[str]:
    return [k for k, v in results.items() if v["status"] in ("failed", "missing")]


# ---------------------------------------------------------------------------
# Recording and alerts
# ---------------------------------------------------------------------------

def save_results(cur, ds: date, results: dict[str, dict]) -> None:
    cur.execute(DDL)
    for dataset_id, r in results.items():
        cur.execute(
            """INSERT INTO data_detector.daily_flow_checks
                   (check_date, dataset_id, layer, status, blocked_by, details, checked_at)
               VALUES (%s, %s, %s, %s, %s, %s, now())
               ON CONFLICT (check_date, dataset_id) DO UPDATE SET
                   layer = EXCLUDED.layer, status = EXCLUDED.status,
                   blocked_by = EXCLUDED.blocked_by, details = EXCLUDED.details,
                   checked_at = now()""",
            (ds, dataset_id, r["layer"], r["status"], r["blocked_by"],
             json.dumps({**r["details"], **({"severity": r["severity"]} if r.get("severity") else {})},
                        default=str)),
        )


def _split(dataset_id: str) -> tuple[str, str]:
    schema, _, name = dataset_id.partition(".")
    return schema, name


def sync_events(cur, ds: date, results: dict[str, dict], registry: Registry) -> dict[str, list[str]]:
    """Open one event per root failure; resolve events for datasets now ok."""
    cur.execute(
        """SELECT table_schema || '.' || table_name FROM data_detector.data_quality_events
           WHERE check_name = %s AND status IN ('open', 'failed')""", (CHECK_NAME,))
    open_ids = {r[0] for r in cur.fetchall()}
    opened, resolved = [], []
    for dataset_id in roots(results):
        if dataset_id in open_ids:
            continue
        schema, name = _split(dataset_id)
        r = results[dataset_id]
        cur.execute(
            """INSERT INTO data_detector.data_quality_events
                   (dag_id, task_id, severity, check_name, table_schema, table_name, status, details, error_message)
               VALUES (%s, %s, %s, %s, %s, %s, 'open', %s, %s)""",
            (registry[dataset_id].producer_dag, registry[dataset_id].producer_task,
             effective_severity(dataset_id, results, registry), CHECK_NAME,
             schema, name,
             json.dumps({"check_date": str(ds), "status": r["status"], **r["details"],
                         "blocks": registry.downstream(dataset_id)}, default=str),
             f"{dataset_id} {r['status']} for {ds}"),
        )
        opened.append(dataset_id)
    for dataset_id in sorted(open_ids):
        if results.get(dataset_id, {}).get("status") == "ok":
            schema, name = _split(dataset_id)
            cur.execute(
                """UPDATE data_detector.data_quality_events
                   SET status = 'resolved',
                       details = details || jsonb_build_object('resolution',
                           jsonb_build_object('resolved_at', now(), 'check_date', %s::text,
                                              'reason', 'data flowed'))
                   WHERE check_name = %s AND table_schema = %s AND table_name = %s
                     AND status IN ('open', 'failed')""",
                (str(ds), CHECK_NAME, schema, name),
            )
            resolved.append(dataset_id)
    return {"opened": opened, "resolved": resolved}


def format_alert(ds: date, results: dict[str, dict], registry: Registry) -> tuple[str, str]:
    bad = roots(results)
    blocked = [k for k, v in results.items() if v["status"] == "blocked"]
    subject = f"Ballistics data flow {ds}: {len(bad)} failed, {len(blocked)} blocked"
    lines = [f"Day checked: {ds} (Airflow logical date)", ""]
    for dataset_id in bad:
        r, d = results[dataset_id], registry[dataset_id]
        producer = f"{d.producer_dag}.{d.producer_task}" if d.producer_dag else "-"
        mark = "!" if effective_severity(dataset_id, results, registry) == "warning" else "✗"
        lines.append(f"{mark} {dataset_id} — {r['status']} (producer {producer})")
        for key, value in r["details"].items():
            lines.append(f"    {key}: {value}")
        downstream = [x for x in registry.downstream(dataset_id) if results[x]["status"] == "blocked"]
        if downstream:
            lines.append(f"    blocks: {', '.join(downstream)}")
    ok = sum(1 for v in results.values() if v["status"] == "ok")
    lines += ["", f"{ok} of {len(results)} datasets ok."]
    return subject, "\n".join(lines)


METRIC_SQL = {
    "match_pages": ("SELECT n_fixtures, to_char(COALESCE(generated_at, finished_at) AT TIME ZONE 'UTC', "
                    "'Dy HH24:MI \"UTC\"') FROM raw.engine_runs WHERE run_ds = %s AND status = 'complete'"),
    "pool_rows": "SELECT count(*), NULL FROM raw.consensus_pool WHERE run_ds = %s",
    "crest_coverage": """
        SELECT count(*) FILTER (WHERE logo IS NOT NULL), count(*) FROM (
          SELECT DISTINCT v.id, v.logo FROM raw.fact_fixtures f
          CROSS JOIN LATERAL (VALUES (home_team_id, home_logo_file_id),
                                     (away_team_id, away_logo_file_id)) v(id, logo)
          WHERE v.id IS NOT NULL
            AND f.kickoff_utc >= now() AND f.kickoff_utc < now() + interval '14 days') t""",
}


def collect_metrics(cur, ds: date, results: dict[str, dict]) -> dict:
    """Headline numbers for the daily report. A missing table just leaves a dash."""
    metrics: dict = {"fixtures_loaded": (results.get("raw.raw_fixtures") or {}).get("details", {}).get("rows")}
    for key, sql in METRIC_SQL.items():
        cur.execute("SAVEPOINT flow_metric")
        try:
            cur.execute(sql, (ds,) if "%s" in sql else None)
            row = cur.fetchone()
            cur.execute("RELEASE SAVEPOINT flow_metric")
        except Exception as exc:
            cur.execute("ROLLBACK TO SAVEPOINT flow_metric")
            logger.warning("daily_flow metric %s unavailable: %s", key, exc)
            continue
        if not row:
            continue
        if key == "match_pages":
            metrics["match_pages"], metrics["engine_built"] = row[0], (f"built {row[1]}" if row[1] else "")
        elif key == "crest_coverage":
            metrics["crest_coverage"] = (row[0], row[1]) if row[1] else None
        else:
            metrics[key] = row[0]
    return metrics


ACTIVITY_SQL = {
    # Per DAG for the day: tasks, outcomes, wall-clock window, slowest task.
    "runs": """
        WITH day AS (
          SELECT dag_id, task_id, status, started_at, finished_at FROM data_detector.pipeline_runs
          WHERE ((metadata->>'logical_date')::timestamptz AT TIME ZONE 'UTC')::date = %(ds)s)
        SELECT dag_id, count(*), count(*) FILTER (WHERE status = 'success'),
               count(*) FILTER (WHERE status IN ('failed', 'upstream_failed')),
               count(*) FILTER (WHERE status = 'skipped'),
               min(started_at), max(finished_at),
               (SELECT d2.task_id FROM day d2 WHERE d2.dag_id = day.dag_id AND d2.finished_at IS NOT NULL
                ORDER BY d2.finished_at - d2.started_at DESC NULLS LAST LIMIT 1),
               (SELECT EXTRACT(EPOCH FROM max(d2.finished_at - d2.started_at)) FROM day d2
                WHERE d2.dag_id = day.dag_id)
        FROM day GROUP BY dag_id ORDER BY dag_id""",
    # Rows moved per movement and target for the day, against the median of the 7 days
    # before (a median, so one bootstrap load does not make every normal day look low).
    "movements": """
        WITH m AS (
          SELECT (r.logical_date::timestamptz AT TIME ZONE 'UTC')::date AS day,
                 m.movement_type, coalesce(m.target_schema || '.' || m.target_table, m.source_system) AS target,
                 m.status, m.row_count, m.inserted_count, m.updated_count, m.failed_count
          FROM data_detector.data_movements m
          JOIN (SELECT DISTINCT dag_id, run_id, metadata->>'logical_date' AS logical_date
                FROM data_detector.pipeline_runs) r ON r.dag_id = m.dag_id AND r.run_id = m.run_id
          WHERE (r.logical_date::timestamptz AT TIME ZONE 'UTC')::date
                BETWEEN %(ds)s::date - 7 AND %(ds)s),
        today AS (
          SELECT movement_type, target, sum(row_count) AS rows, sum(inserted_count) AS ins,
                 sum(updated_count) AS upd, sum(failed_count) AS failed,
                 count(*) FILTER (WHERE status <> 'success') AS bad_moves
          FROM m WHERE day = %(ds)s GROUP BY 1, 2),
        prior AS (
          SELECT movement_type, target,
                 percentile_cont(0.5) WITHIN GROUP (ORDER BY rows) AS median_rows FROM (
            SELECT day, movement_type, target, sum(row_count) AS rows FROM m
            WHERE day < %(ds)s GROUP BY 1, 2, 3) x GROUP BY 1, 2)
        SELECT t.movement_type, t.target, t.rows, t.ins, t.upd, t.failed, t.bad_moves, p.median_rows
        FROM today t LEFT JOIN prior p USING (movement_type, target)
        ORDER BY t.movement_type, t.target""",
    # Incidents still open, oldest first.
    "incidents": """
        SELECT coalesce(table_schema || '.' || table_name, check_name), check_name, severity,
               EXTRACT(EPOCH FROM now() - detected_at) / 3600, error_message
        FROM data_detector.data_quality_events
        WHERE status IN ('open', 'failed')
        ORDER BY detected_at LIMIT 15""",
}


def _num(x):
    return None if x is None else float(x)


def collect_activity(cur, ds: date) -> dict:
    """What ran and moved on ds, plus open incidents. A failed query leaves its key out."""
    out: dict = {}
    for key, sql in ACTIVITY_SQL.items():
        cur.execute("SAVEPOINT flow_activity")
        try:
            cur.execute(sql, {"ds": ds})
            rows = cur.fetchall()
            cur.execute("RELEASE SAVEPOINT flow_activity")
        except Exception as exc:
            cur.execute("ROLLBACK TO SAVEPOINT flow_activity")
            logger.warning("daily_flow activity %s unavailable: %s", key, exc)
            continue
        if key == "runs":
            out[key] = [{"dag": r[0], "tasks": r[1], "ok": r[2], "failed": r[3], "skipped": r[4],
                         "started": r[5], "finished": r[6], "slowest_task": r[7],
                         "slowest_seconds": _num(r[8])} for r in rows]
        elif key == "movements":
            out[key] = [{"movement": r[0], "target": r[1], "rows": r[2], "inserted": r[3],
                         "updated": r[4], "failed": r[5] or 0, "bad_moves": r[6] or 0,
                         "median_rows": _num(r[7])} for r in rows]
        else:
            out[key] = [{"dataset": r[0], "check": r[1], "severity": r[2],
                         "age_hours": _num(r[3]) or 0.0, "error": (r[4] or "")[:200]} for r in rows]
    return out


def collect_history(cur, ds: date, days: int = 7) -> list[dict]:
    """Worst status per day for the last `days` days ending at ds (oldest first)."""
    cur.execute(
        """SELECT c.check_date,
                  CASE WHEN bool_or(c.status IN ('failed', 'missing')
                                    AND coalesce(c.details->>'severity', d.check_spec->>'severity', 'error')
                                        <> 'warning') THEN 'failed'
                       WHEN bool_or(c.status IN ('failed', 'missing')) THEN 'warning'
                       WHEN bool_or(c.status = 'blocked') THEN 'blocked'
                       ELSE 'ok' END
           FROM data_detector.daily_flow_checks c
           LEFT JOIN data_detector.datasets d USING (dataset_id)
           WHERE c.check_date > %s::date - %s AND c.check_date <= %s
           GROUP BY c.check_date""", (ds, days, ds))
    by_day = {r[0]: r[1] for r in cur.fetchall()}
    out = []
    for i in range(days - 1, -1, -1):
        day = ds - timedelta(days=i)
        out.append({"date": day, "label": day.strftime("%a %d"), "status": by_day.get(day, "none")})
    return out


def daily_report_enabled() -> bool:
    return os.getenv("ALERT_DAILY_REPORT", "true").strip().lower() not in ("0", "false", "no", "off")


def run(ds: date, *, conn=None, registry: Registry | None = None, notify: bool = True,
        task_state=None) -> dict:
    """Check one day end to end. Returns the summary; raises nothing on data problems.

    task_state(dag_id, task_id, ds) -> state | None: fallback for pipeline_task
    checks when pipeline_runs has no row (the DAG passes Airflow's own state)."""
    from etl.src.data_detector.email_templates import render
    from etl.src.data_detector.notify import send_alert
    registry = registry or load_registry()
    own = conn is None
    if own:
        from etl.src.extract_metadata import get_db_connection
        conn = get_db_connection()
    try:
        sync_registry(conn, registry)
        with conn.cursor() as cur:
            results = evaluate(cur, registry, ds, task_state)
        conn.rollback()                      # reads only above; start clean for writes
        with conn.cursor() as cur:
            save_results(cur, ds, results)
            events = sync_events(cur, ds, results, registry)
        conn.commit()
        with conn.cursor() as cur:
            metrics = collect_metrics(cur, ds, results)
            history = collect_history(cur, ds)
            from etl.src.data_detector.model_watch import collect as collect_watch
            watch = collect_watch(cur, ds)
            activity = collect_activity(cur, ds)
        conn.rollback()
    finally:
        if own:
            conn.close()
    bad = roots(results)
    warnings = [d for d in bad if effective_severity(d, results, registry) == "warning"]
    errors = [d for d in bad if d not in warnings]
    sent: list[str] = []
    # Problems and recoveries always send; a clean day sends only with the daily report on.
    if notify and (bad or events["resolved"] or daily_report_enabled()):
        subject, text, html = render(ds, results, registry, metrics=metrics, history=history,
                                     resolved=events["resolved"], watch=watch,
                                     activity=activity)
        sent = send_alert(subject, text, html=html)
    summary = {"check_date": str(ds), "ok": sum(v["status"] == "ok" for v in results.values()),
               "failed_or_missing": errors,          # fails the Airflow task
               "warnings": warnings,                 # alerted, does not fail the task
               "blocked": [k for k, v in results.items() if v["status"] == "blocked"],
               "events": events, "metrics": metrics, "model_watch": watch,
               "alert_channels": sent}
    print(json.dumps(summary, indent=2, default=str))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily data flow check.")
    parser.add_argument("ds", help="Airflow logical date to check (YYYY-MM-DD)")
    parser.add_argument("--no-notify", action="store_true")
    args = parser.parse_args()
    summary = run(date.fromisoformat(args.ds), notify=not args.no_notify)
    raise SystemExit(1 if summary["failed_or_missing"] else 0)


if __name__ == "__main__":
    main()
