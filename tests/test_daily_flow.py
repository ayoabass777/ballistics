"""Dataset registry and daily flow check (etl/src/data_detector)."""
import os
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from etl.src.data_detector import datasets as dsets
from etl.src.data_detector import daily_flow as flow
from etl.src.data_detector import notify

DS = date(2026, 10, 13)
ROOT = Path(__file__).resolve().parents[1]


def mini_registry(extra=None):
    items = [
        {"id": "api.a", "layer": "source", "upstream": [], "check": {"type": "derived"}},
        {"id": "raw.b", "layer": "raw", "upstream": ["api.a"], "check": {"type": "derived"}},
        {"id": "raw.c", "layer": "engine", "upstream": ["raw.b"], "check": {"type": "derived"}},
    ] + (extra or [])
    assert not dsets.validate({"datasets": items})
    return dsets.Registry(dsets.Dataset(i["id"], i["layer"], "", None, None,
                                        tuple(i["upstream"]), i["check"]) for i in items)


class RegistryTests(unittest.TestCase):
    def test_shipped_registry_is_valid(self):
        reg = dsets.load_registry()
        self.assertIn("raw.match_analysis", reg.datasets)
        order = reg.topological()
        self.assertLess(order.index("raw.raw_fixtures"), order.index("dbt.run"))
        self.assertLess(order.index("dbt.run"), order.index("raw.model_runs"))

    def test_lineage(self):
        reg = dsets.load_registry()
        down = reg.downstream("raw.raw_fixtures")
        for d in ("dbt.run", "raw.fact_fixtures", "raw.consensus_pool", "api.match_page"):
            self.assertIn(d, down)
        self.assertIn("api.fixture_updates", reg.upstream_all("raw.match_analysis"))

    def test_validation_errors(self):
        bad = {"datasets": [
            {"id": "x", "layer": "raw", "upstream": ["y"], "check": {"type": "derived"}},
            {"id": "y", "layer": "raw", "upstream": ["x"], "check": {"type": "derived"}},
        ]}
        self.assertEqual(dsets.validate(bad), ["dependency cycle"])
        self.assertTrue(any("unknown upstream" in e for e in dsets.validate(
            {"datasets": [{"id": "x", "layer": "raw", "upstream": ["nope"], "check": {"type": "derived"}}]})))
        self.assertTrue(any("pipeline_task needs" in e for e in dsets.validate(
            {"datasets": [{"id": "x", "layer": "raw", "check": {"type": "pipeline_task"}}]})))

    def test_unsafe_identifier_rejected(self):
        with self.assertRaises(ValueError):
            flow._ident("raw.x; drop table y")


class BlameTests(unittest.TestCase):
    def test_root_and_blocked(self):
        reg = mini_registry()
        results = {"api.a": {"status": "missing", "details": {}, "blocked_by": [], "layer": "source"}}

        class Cur:  # derived checks never query; patch evaluation of api.a
            def execute(self, *a): pass

        with patch.dict(flow.CHECKS, {"derived": None}):
            pass
        reg["api.a"].check["type"] = "run_ds_rows"
        with patch.object(flow, "CHECKS", {"run_ds_rows": lambda cur, spec, ds: {"status": "missing", "details": {}}}):
            results = flow.evaluate(Cur(), reg, DS)
        self.assertEqual(results["api.a"]["status"], "missing")
        self.assertEqual(results["raw.b"]["status"], "blocked")
        self.assertEqual(results["raw.c"]["blocked_by"], ["api.a"])
        self.assertEqual(flow.roots(results), ["api.a"])
        subject, body = flow.format_alert(DS, results, reg)
        self.assertIn("1 failed, 2 blocked", subject)
        self.assertIn("blocks: raw.b, raw.c", body)


class NotifyTests(unittest.TestCase):
    def test_no_channel_logs_only(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(notify.send_alert("s", "b"), [])

    def test_slack_failure_does_not_stop_email(self):
        env = {"ALERT_SLACK_WEBHOOK_URL": "https://hook", "ALERT_EMAIL_TO": "a@b.c", "SMTP_HOST": "smtp"}
        with patch.dict(os.environ, env, clear=True), \
                patch.dict(notify.SENDERS, {"slack": lambda s, b: (_ for _ in ()).throw(OSError("down")),
                                            "email": lambda s, b: None}):
            self.assertEqual(notify.send_alert("s", "b"), ["email"])


@unittest.skipUnless(os.getenv("FLOW_TEST_DSN"), "requires FLOW_TEST_DSN (throwaway database)")
class PostgresFlowTests(unittest.TestCase):
    """End to end against a real database: schema, checks, history, events."""

    def setUp(self):
        import psycopg2
        self.conn = psycopg2.connect(os.environ["FLOW_TEST_DSN"])
        cur = self.conn.cursor()
        cur.execute("DROP SCHEMA IF EXISTS data_detector CASCADE; DROP SCHEMA IF EXISTS raw CASCADE;"
                    "DROP SCHEMA IF EXISTS raw_mart CASCADE; CREATE SCHEMA raw; CREATE SCHEMA raw_mart;")
        cur.execute((ROOT / "etl/sql/bootstrap_data_detector_schema.sql").read_text())
        cur.execute("""
            CREATE TABLE raw.model_runs (run_ds date, model text, status text);
            CREATE TABLE raw.model_picks (run_ds date);
            CREATE TABLE raw.consensus_pool (run_ds date);
            CREATE TABLE raw.engine_runs (run_ds date, status text);
        """)  # raw.match_analysis deliberately absent
        self.conn.commit()
        self.registry = dsets.load_registry()

    def tearDown(self):
        self.conn.close()

    def pipeline(self, dag, task, status, ds=DS):
        cur = self.conn.cursor()
        logical = datetime(ds.year, ds.month, ds.day, tzinfo=timezone.utc).isoformat()
        cur.execute("""INSERT INTO data_detector.pipeline_runs (run_id, dag_id, task_id, status, metadata)
                       VALUES (%s, %s, %s, %s, jsonb_build_object('logical_date', %s::text))
                       ON CONFLICT (dag_id, task_id, run_id) DO UPDATE SET status = EXCLUDED.status""",
                    (f"scheduled__{ds}", dag, task, status, logical))
        self.conn.commit()

    def movement(self, mtype, status):
        self.pipeline("fixture_update_dag", "extract_updates", "success")
        cur = self.conn.cursor()
        cur.execute("""INSERT INTO data_detector.data_movements (run_id, dag_id, task_id, movement_type, source_system, row_count, status)
                       VALUES (%s, 'fixture_update_dag', 't', %s, 'x', 40, %s)""", (f"scheduled__{DS}", mtype, status))
        self.conn.commit()

    def full_etl(self):
        self.movement("api_to_s3", "success")
        self.movement("s3_to_raw", "success")
        for t in ("fixture_corrections", "extract_xg_task", "update_standings_task", "run_dbt"):
            self.pipeline("fixture_update_dag", t, "success")

    def engine(self, models=3):
        cur = self.conn.cursor()
        for m in ("mismatch", "bayesian", "dixon_coles")[:models]:
            cur.execute("INSERT INTO raw.model_runs VALUES (%s, %s, 'ok')", (DS, m))
        cur.execute("INSERT INTO raw.model_picks VALUES (%s)", (DS,))
        cur.execute("INSERT INTO raw.consensus_pool VALUES (%s)", (DS,))
        cur.execute("INSERT INTO raw.engine_runs VALUES (%s, 'complete')", (DS,))
        self.conn.commit()

    def run_check(self):
        with patch.object(notify, "send_alert", return_value=[]) as alert:
            summary = flow.run(DS, conn=self.conn, registry=self.registry)
        return summary, alert

    def statuses(self):
        cur = self.conn.cursor()
        cur.execute("SELECT dataset_id, status FROM data_detector.daily_flow_checks WHERE check_date = %s", (DS,))
        return dict(cur.fetchall())

    def test_dbt_failure_blocks_engine_and_alerts(self):
        self.movement("api_to_s3", "success")
        self.movement("s3_to_raw", "success")
        for t in ("fixture_corrections", "extract_xg_task", "update_standings_task"):
            self.pipeline("fixture_update_dag", t, "success")
        self.pipeline("fixture_update_dag", "run_dbt", "failed")
        summary, alert = self.run_check()
        st = self.statuses()
        self.assertEqual(st["raw.raw_fixtures"], "ok")
        self.assertEqual(st["dbt.run"], "failed")
        self.assertEqual(st["raw.consensus_pool"], "blocked")
        self.assertEqual(st["api.match_page"], "blocked")
        self.assertEqual(summary["failed_or_missing"], ["dbt.run"])     # blame lands on the root only
        alert.assert_called_once()
        cur = self.conn.cursor()
        cur.execute("SELECT count(*) FROM data_detector.datasets")
        self.assertEqual(cur.fetchone()[0], len(self.registry.datasets))

    def test_missing_table_is_missing_and_recovery_resolves(self):
        self.full_etl()
        self.engine()
        summary, _ = self.run_check()
        self.assertEqual(summary["failed_or_missing"], ["raw.match_analysis"])  # table absent
        self.assertEqual(summary["events"]["opened"], ["raw.match_analysis"])
        summary, _ = self.run_check()                                           # rerun: no duplicate event
        self.assertEqual(summary["events"]["opened"], [])
        cur = self.conn.cursor()
        cur.execute("CREATE TABLE raw.match_analysis (run_ds date); INSERT INTO raw.match_analysis VALUES (%s)", (DS,))
        self.conn.commit()
        summary, alert = self.run_check()
        self.assertEqual(summary["failed_or_missing"], [])
        self.assertEqual(summary["events"]["resolved"], ["raw.match_analysis"])
        alert.assert_called_once()                                              # recovery note
        self.assertTrue(all(v == "ok" for v in self.statuses().values()))

    def test_partial_models_is_missing(self):
        self.full_etl()
        self.engine(models=2)
        summary, _ = self.run_check()
        self.assertIn("raw.model_runs", summary["failed_or_missing"])


if __name__ == "__main__":
    unittest.main()
