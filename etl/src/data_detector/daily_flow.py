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
from etl.src.data_detector.datasets import Registry, load_registry, sync_registry

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


def check_pipeline_task(cur, spec: dict, ds: date) -> dict:
    cur.execute(PIPELINE_SQL, (spec["dag"], spec["task"], ds))
    row = cur.fetchone()
    if row is None:
        return {"status": "missing", "details": {"reason": "task did not run for this day"}}
    status, finished_at, error = row
    if status == "success":
        return {"status": "ok", "details": {"finished_at": str(finished_at)}}
    if status == "skipped":
        return {"status": "ok", "details": {"note": "task skipped by the DAG"}}
    if status in ("failed", "upstream_failed"):
        return {"status": "failed", "details": {"error": (error or "")[:300]}}
    return {"status": "missing", "details": {"reason": f"task state {status}"}}


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


def check_crest_urls(cur, spec: dict, ds: date) -> dict:
    """HEAD a sample of public crest URLs: each must answer 200 with an image."""
    import requests
    cur.execute(spec["sql"])                       # returns (kind, logo_file_id) rows
    rows = cur.fetchall()
    if not rows:
        return {"status": "missing", "details": {"reason": "no crests to sample"}}
    base, broken = crest_base_url(), []
    for kind, file_id in rows:
        url = f"{base}/crests/{kind}/{file_id}"
        try:
            r = requests.head(url, timeout=10)
            ctype = r.headers.get("Content-Type", "")
            if r.status_code != 200 or not ctype.startswith("image/"):
                broken.append(f"{kind}/{file_id}: {r.status_code} {ctype}".strip())
        except requests.RequestException as exc:
            broken.append(f"{kind}/{file_id}: {type(exc).__name__}")
    details = {"sampled": len(rows), "base_url": base}
    if broken:
        return {"status": "failed", "details": {**details, "broken": broken[:5]}}
    return {"status": "ok", "details": details}


CHECKS = {"pipeline_task": check_pipeline_task, "movement": check_movement,
          "run_ds_rows": check_run_ds_rows, "sql_assert": check_sql_assert,
          "crest_urls": check_crest_urls}


def evaluate(cur, registry: Registry, ds: date) -> dict[str, dict]:
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
            (ds, dataset_id, r["layer"], r["status"], r["blocked_by"], json.dumps(r["details"], default=str)),
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
             registry[dataset_id].check.get("severity", "error"), CHECK_NAME,
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
        mark = "!" if d.check.get("severity") == "warning" else "✗"
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


def collect_history(cur, ds: date, days: int = 7) -> list[dict]:
    """Worst status per day for the last `days` days ending at ds (oldest first)."""
    cur.execute(
        """SELECT c.check_date,
                  CASE WHEN bool_or(c.status IN ('failed', 'missing')
                                    AND coalesce(d.check_spec->>'severity', 'error') <> 'warning') THEN 'failed'
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


def run(ds: date, *, conn=None, registry: Registry | None = None, notify: bool = True) -> dict:
    """Check one day end to end. Returns the summary; raises nothing on data problems."""
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
            results = evaluate(cur, registry, ds)
        conn.rollback()                      # reads only above; start clean for writes
        with conn.cursor() as cur:
            save_results(cur, ds, results)
            events = sync_events(cur, ds, results, registry)
        conn.commit()
        with conn.cursor() as cur:
            metrics = collect_metrics(cur, ds, results)
            history = collect_history(cur, ds)
        conn.rollback()
    finally:
        if own:
            conn.close()
    bad = roots(results)
    warnings = [d for d in bad if registry[d].check.get("severity") == "warning"]
    errors = [d for d in bad if d not in warnings]
    sent: list[str] = []
    # Problems and recoveries always send; a clean day sends only with the daily report on.
    if notify and (bad or events["resolved"] or daily_report_enabled()):
        subject, text, html = render(ds, results, registry, metrics=metrics, history=history,
                                     resolved=events["resolved"])
        sent = send_alert(subject, text, html=html)
    summary = {"check_date": str(ds), "ok": sum(v["status"] == "ok" for v in results.values()),
               "failed_or_missing": errors,          # fails the Airflow task
               "warnings": warnings,                 # alerted, does not fail the task
               "blocked": [k for k, v in results.items() if v["status"] == "blocked"],
               "events": events, "metrics": metrics, "alert_channels": sent}
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
