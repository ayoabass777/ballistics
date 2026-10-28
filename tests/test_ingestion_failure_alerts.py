"""Failed ingestion tasks must reach the detector and the immediate alert channels."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

from etl.src.data_detector import notify
from etl.src.data_detector.failure_alerts import pipeline_run_failed_with_alert


def test_failed_task_records_failure_and_alerts_immediately():
    context = {
        "task_instance": SimpleNamespace(dag_id="fixture_update_dag", task_id="extract_updates", run_id="scheduled__x"),
        "exception": RuntimeError("API down"),
    }
    with patch("etl.src.data_detector.failure_alerts.pipeline_run_failed") as record, \
         patch.object(notify, "send_failure_alert", return_value=["slack", "email"]) as send:
        pipeline_run_failed_with_alert(context)
    record.assert_called_once_with(context)
    assert "fixture_update_dag.extract_updates" in send.call_args.args[0]
    assert "API down" in send.call_args.args[1]


def test_immediate_alert_tries_each_configured_channel_once():
    slack, email = Mock(), Mock()
    with patch.object(notify, "configured_channels", return_value=["slack", "email"]), \
         patch.dict(notify.SENDERS, {"slack": slack, "email": email}):
        assert notify.send_failure_alert("failure", "details") == ["slack", "email"]
    slack.assert_called_once_with("failure", "details")
    email.assert_called_once_with("failure", "details")


def test_immediate_alert_still_emails_when_slack_fails():
    slack = Mock(side_effect=ConnectionError("webhook down"))
    email = Mock()
    with patch.object(notify, "configured_channels", return_value=["slack", "email"]), \
         patch.dict(notify.SENDERS, {"slack": slack, "email": email}):
        assert notify.send_failure_alert("failure", "details") == ["email"]
    slack.assert_called_once()
    email.assert_called_once_with("failure", "details")


def test_immediate_alert_without_channels_sends_nothing():
    with patch.object(notify, "configured_channels", return_value=[]):
        assert notify.send_failure_alert("failure", "details") == []
