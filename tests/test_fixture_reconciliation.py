from unittest.mock import patch
from datetime import datetime, timezone

from etl.src.fixture_correction import fixture_correction as correction


def fixture(fixture_id, status="NS"):
    return {"fixture": {"id": fixture_id, "status": {"short": status}}}


def test_correction_updates_postponed_status_even_when_kickoff_is_unchanged():
    kickoff = datetime(2026, 10, 26, 18, 5, tzinfo=timezone.utc)
    provider = {"fixture": {"id": 1497734, "date": kickoff.isoformat(),
                            "status": {"short": "PST"}}}
    with patch.object(correction, "get_db_kickoff_utc", return_value=(kickoff, "NS")), \
         patch.object(correction, "update_db_fixture_state", return_value=True) as update:
        changes = correction._process_fixtures([provider])
    update.assert_called_once_with(1497734, kickoff.isoformat(), "PST")
    assert changes[0]["old_status"] == "NS"
    assert changes[0]["new_status"] == "PST"


def test_correction_does_not_reopen_finished_fixture():
    kickoff = datetime(2026, 10, 26, 18, 5, tzinfo=timezone.utc)
    provider = {"fixture": {"id": 1497734, "date": kickoff.isoformat(),
                            "status": {"short": "NS"}}}
    with patch.object(correction, "get_db_kickoff_utc", return_value=(kickoff, "FT")), \
         patch.object(correction, "update_db_fixture_state") as update:
        assert correction._process_fixtures([provider]) == []
    update.assert_not_called()


def test_reconciliation_restores_reappearing_orphan():
    with patch.object(correction, "get_league_seasons", return_value=[(365, 2026)]), \
         patch.object(correction, "fetch_fixtures", return_value=[fixture(1515939)]), \
         patch.object(correction, "_process_fixtures", return_value=[]), \
         patch.object(correction, "_db_orphan_fixture_ids_in_window", return_value=["1515939"]), \
         patch.object(correction, "_db_ns_fixture_ids_in_window", return_value=[]), \
         patch.object(correction, "mark_fixtures_status", return_value=1) as mark:
        changes = correction.reconcile_upcoming_fixtures()

    mark.assert_called_once_with(["1515939"], "NS")
    assert changes == [{"fixture_id": "1515939", "action": "restored"}]


def test_reconciliation_retains_fixture_when_direct_lookup_still_finds_it():
    def provider(_endpoint, params, **_kwargs):
        return [fixture(2)] if "id" not in params else [fixture(1)]

    with patch.object(correction, "get_league_seasons", return_value=[(365, 2026)]), \
         patch.object(correction, "fetch_fixtures", side_effect=provider) as fetch, \
         patch.object(correction, "_process_fixtures", return_value=[]), \
         patch.object(correction, "_db_orphan_fixture_ids_in_window", return_value=[]), \
         patch.object(correction, "_db_ns_fixture_ids_in_window", return_value=["1", "2"]), \
         patch.object(correction, "mark_fixtures_status") as mark:
        changes = correction.reconcile_upcoming_fixtures()

    assert changes == []
    mark.assert_not_called()
    assert fetch.call_args_list[-1].kwargs == {"raise_on_failure": True}


def test_reconciliation_marks_orphan_only_after_empty_direct_lookup():
    def provider(_endpoint, params, **_kwargs):
        return [fixture(2)] if "id" not in params else []

    with patch.object(correction, "get_league_seasons", return_value=[(365, 2026)]), \
         patch.object(correction, "fetch_fixtures", side_effect=provider), \
         patch.object(correction, "_process_fixtures", return_value=[]), \
         patch.object(correction, "_db_orphan_fixture_ids_in_window", return_value=[]), \
         patch.object(correction, "_db_ns_fixture_ids_in_window", return_value=["1", "2"]), \
         patch.object(correction, "mark_fixtures_status", return_value=1) as mark:
        changes = correction.reconcile_upcoming_fixtures()

    mark.assert_called_once_with(["1"], correction.ORPHAN_STATUS)
    assert changes == [{"fixture_id": "1", "action": "orphaned"}]
