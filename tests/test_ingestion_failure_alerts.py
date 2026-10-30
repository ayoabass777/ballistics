"""Failures in ingestion must reach Airflow and its immediate alert callback."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from etl.src import update_fixtures, update_standings
from etl.src.data_detector import notify
from etl.src.data_detector.failure_alerts import pipeline_run_failed_with_alert
from etl.src import extract_fixtures
from etl.src.extract_fixtures import ApiResponseError


def test_bootstrap_fetch_rejects_empty_success_and_propagates_transport_failure():
    with patch.object(extract_fixtures, "fetch_fixtures", return_value=[]) as fetch:
        with pytest.raises(ApiResponseError, match="No fixtures returned"):
            extract_fixtures.fetch_bootstrap_fixtures(39, 2026)
    assert fetch.call_args.kwargs["raise_on_failure"] is True

    with patch.object(extract_fixtures, "fetch_fixtures", side_effect=ApiResponseError("API down")):
        with pytest.raises(ApiResponseError, match="API down"):
            extract_fixtures.fetch_bootstrap_fixtures(39, 2026)


def test_fixture_selector_database_error_is_not_treated_as_no_work():
    conn = Mock()
    conn.cursor.return_value.execute.side_effect = RuntimeError("staging unavailable")
    with patch.object(update_fixtures, "get_db_connection", return_value=conn):
        with pytest.raises(RuntimeError, match="staging unavailable"):
            update_fixtures.to_update_fixture_ids()
    conn.close.assert_called_once()


def test_fixture_batch_transport_failure_fails_the_extraction():
    with patch.object(update_fixtures, "fetch_fixtures", side_effect=ApiResponseError("API down")) as fetch:
        with pytest.raises(RuntimeError, match="1/1 fixture-update batches failed"):
            update_fixtures.update_by_ids(["123"])
    assert fetch.call_args.kwargs["raise_on_failure"] is True


def test_partial_fixture_batch_failure_is_not_reported_as_success():
    with patch.object(update_fixtures, "fetch_fixtures", side_effect=[[], ApiResponseError("API down")]):
        with pytest.raises(RuntimeError, match="1/2 fixture-update batches failed"):
            update_fixtures.update_by_ids([str(i) for i in range(21)])


def test_empty_fixture_response_for_requested_ids_fails():
    with patch.object(update_fixtures, "fetch_fixtures", return_value=[]):
        with pytest.raises(RuntimeError, match="no fixture updates"):
            update_fixtures.update_by_ids(["123"])


def test_standings_transport_failure_is_not_an_empty_table():
    with patch.object(update_standings, "_rate_limited_get", side_effect=ApiResponseError("API down")) as fetch:
        with pytest.raises(ApiResponseError, match="API down"):
            update_standings.fetch_standings(39, 2026)
    assert fetch.call_args.kwargs["raise_on_failure"] is True


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


def _season_cursor(start, end):
    cur = Mock()
    cur.fetchone.return_value = (start, end) if start or end else None
    return cur


def test_empty_season_is_expected_only_outside_its_dates():
    from datetime import date
    today = date(2026, 10, 28)
    finished = _season_cursor(date(2026, 7, 11), date(2026, 8, 29))
    not_started = _season_cursor(date(2026, 11, 1), date(2027, 5, 1))
    in_progress = _season_cursor(date(2026, 8, 7), date(2027, 6, 6))
    no_dates = _season_cursor(None, None)
    assert extract_fixtures.empty_season_is_expected(finished, 1, today) is True
    assert extract_fixtures.empty_season_is_expected(not_started, 1, today) is True
    assert extract_fixtures.empty_season_is_expected(in_progress, 1, today) is False
    assert extract_fixtures.empty_season_is_expected(no_dates, 1, today) is False


def test_empty_bootstrap_season_is_distinguishable_from_api_failure():
    with patch.object(extract_fixtures, "fetch_fixtures", return_value=[]):
        with pytest.raises(extract_fixtures.EmptySeasonError):
            extract_fixtures.fetch_bootstrap_fixtures(1147, 2026)
    with patch.object(extract_fixtures, "fetch_fixtures", side_effect=ApiResponseError("API down")):
        with pytest.raises(ApiResponseError) as err:
            extract_fixtures.fetch_bootstrap_fixtures(1147, 2026)
    assert not isinstance(err.value, extract_fixtures.EmptySeasonError)


def test_bootstrap_failure_message_covers_extraction_and_load():
    assert extract_fixtures.bootstrap_failure_message([], []) is None
    message = extract_fixtures.bootstrap_failure_message(["98/2026: API down"], ["fixtures/98.json"])
    assert "extraction failed for 98/2026: API down" in message
    assert "load failed for S3 keys fixtures/98.json" in message


def test_one_league_standings_failure_does_not_stop_the_others():
    from etl.src import standings_dlq
    from etl.src.dlq import LoadResult
    leagues = [(1, 39, 2026), (2, 140, 2026)]
    fetch = Mock(side_effect=[ApiResponseError("API down"), [{"league": {}}]])
    refresh = Mock(return_value=LoadResult(loaded=20))
    with patch.object(update_standings, "get_db_connection", return_value=Mock()), \
         patch.object(update_standings, "get_league_seasons", return_value=leagues), \
         patch.object(update_standings, "fetch_standings", fetch), \
         patch.object(standings_dlq, "refresh_league", refresh):
        with pytest.raises(RuntimeError, match=r"failed for 1/2 leagues: 39/2026: API down"):
            update_standings.update_standings(ds="2026-10-28")
    assert fetch.call_count == 2
    refresh.assert_called_once()
    assert refresh.call_args.args[0]["api_league_id"] == 140


def test_bootstrap_requests_the_provider_season_not_the_start_year():
    cur = Mock()
    cur.fetchall.return_value = []
    extract_fixtures.get_season_rows(cur)
    sql = cur.execute.call_args.args[0]
    assert "ls.season AS season_year" in sql
    assert "start_date" not in sql
