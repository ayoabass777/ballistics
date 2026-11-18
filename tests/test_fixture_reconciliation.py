from unittest.mock import patch
from datetime import datetime, timezone

from etl.src.fixture_correction import fixture_correction as correction


def fixture(fixture_id, status="NS"):
    return {"fixture": {"id": fixture_id, "status": {"short": status}}}


def db_state(kickoff, status, home=(10, "Home FC"), away=(20, "Away FC")):
    return {"kickoff_utc": kickoff, "fixture_status": status,
            "home_team_id": home[0], "home_team_name": home[1],
            "away_team_id": away[0], "away_team_name": away[1]}


def test_correction_updates_postponed_status_even_when_kickoff_is_unchanged():
    kickoff = datetime(2026, 10, 26, 18, 5, tzinfo=timezone.utc)
    provider = {"fixture": {"id": 1497734, "date": kickoff.isoformat(),
                            "status": {"short": "PST"}}}
    with patch.object(correction, "get_db_fixture_state", return_value=db_state(kickoff, "NS")), \
         patch.object(correction, "update_db_fixture_state", return_value=True) as update:
        changes = correction._process_fixtures([provider])
    update.assert_called_once_with(1497734, kickoff.isoformat(), "PST", None)
    assert changes[0]["old_status"] == "NS"
    assert changes[0]["new_status"] == "PST"


def test_correction_does_not_reopen_finished_fixture():
    kickoff = datetime(2026, 10, 26, 18, 5, tzinfo=timezone.utc)
    provider = {"fixture": {"id": 1497734, "date": kickoff.isoformat(),
                            "status": {"short": "NS"}}}
    with patch.object(correction, "get_db_fixture_state", return_value=db_state(kickoff, "FT")), \
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


def test_correction_leaves_full_time_to_the_score_updater():
    # Marking FT here without goals made fixture 1493226 "played" with NULL
    # goals and crashed the Elo refresh on 2026-11-01.
    kickoff = datetime(2026, 10, 31, 22, 15, tzinfo=timezone.utc)
    provider = {"fixture": {"id": 1493226, "date": kickoff.isoformat(),
                            "status": {"short": "FT"}}}
    with patch.object(correction, "get_db_fixture_state", return_value=db_state(kickoff, "NS")), \
         patch.object(correction, "update_db_fixture_state") as update:
        assert correction._process_fixtures([provider]) == []
    update.assert_not_called()


# Livyi Bereh v Shakhtar Donetsk (API 1565534, 20 Nov 2026): stored with
# Shakhtar away; the provider later listed Shakhtar at home. Before this
# check, only kickoff and status were compared before a match was played.
SHAKHTAR, LIVYI_BEREH = (550, "Shakhtar Donetsk"), (17162, "Livyi Bereh")
KICKOFF = datetime(2026, 11, 20, 16, 0, tzinfo=timezone.utc)


def provider_fixture(home, away, status="NS", kickoff=KICKOFF):
    return {"fixture": {"id": 1565534, "date": kickoff.isoformat(), "status": {"short": status}},
            "teams": {"home": {"id": home[0], "name": home[1]},
                      "away": {"id": away[0], "name": away[1]}}}


def test_correction_swaps_home_and_away_when_the_provider_does():
    stored = db_state(KICKOFF, "NS", home=LIVYI_BEREH, away=SHAKHTAR)
    with patch.object(correction, "get_db_fixture_state", return_value=stored), \
         patch.object(correction, "update_db_fixture_state", return_value=True) as update:
        changes = correction._process_fixtures([provider_fixture(SHAKHTAR, LIVYI_BEREH)])
    update.assert_called_once_with(1565534, KICKOFF.isoformat(), "NS", {
        "home_team_id": 550, "home_team_name": "Shakhtar Donetsk",
        "away_team_id": 17162, "away_team_name": "Livyi Bereh"})
    assert changes[0]["old_teams"] == "Livyi Bereh v Shakhtar Donetsk"
    assert changes[0]["new_teams"] == "Shakhtar Donetsk v Livyi Bereh"


def test_correction_leaves_matching_teams_alone():
    stored = db_state(KICKOFF, "NS", home=SHAKHTAR, away=LIVYI_BEREH)
    with patch.object(correction, "get_db_fixture_state", return_value=stored), \
         patch.object(correction, "update_db_fixture_state") as update:
        assert correction._process_fixtures([provider_fixture(SHAKHTAR, LIVYI_BEREH)]) == []
    update.assert_not_called()


def test_team_name_change_alone_is_not_a_swap():
    # Same team ids, new display name: nothing to correct here.
    stored = db_state(KICKOFF, "NS", home=(550, "Shakhtar"), away=LIVYI_BEREH)
    with patch.object(correction, "get_db_fixture_state", return_value=stored), \
         patch.object(correction, "update_db_fixture_state") as update:
        assert correction._process_fixtures([provider_fixture(SHAKHTAR, LIVYI_BEREH)]) == []
    update.assert_not_called()


def test_kickoff_change_keeps_stored_teams_when_sides_match():
    later = datetime(2026, 11, 21, 13, 0, tzinfo=timezone.utc)
    stored = db_state(KICKOFF, "NS", home=SHAKHTAR, away=LIVYI_BEREH)
    with patch.object(correction, "get_db_fixture_state", return_value=stored), \
         patch.object(correction, "update_db_fixture_state", return_value=True) as update:
        changes = correction._process_fixtures([provider_fixture(SHAKHTAR, LIVYI_BEREH, kickoff=later)])
    update.assert_called_once_with(1565534, later.isoformat(), "NS", None)
    assert "new_teams" not in changes[0]


def test_payload_without_team_ids_never_changes_teams():
    stored = db_state(KICKOFF, "NS", home=LIVYI_BEREH, away=SHAKHTAR)
    payload = provider_fixture(SHAKHTAR, LIVYI_BEREH)
    payload["teams"]["home"]["id"] = None
    with patch.object(correction, "get_db_fixture_state", return_value=stored), \
         patch.object(correction, "update_db_fixture_state") as update:
        assert correction._process_fixtures([payload]) == []
    update.assert_not_called()


def test_finished_fixture_teams_are_not_touched():
    stored = db_state(KICKOFF, "FT", home=LIVYI_BEREH, away=SHAKHTAR)
    with patch.object(correction, "get_db_fixture_state", return_value=stored), \
         patch.object(correction, "update_db_fixture_state") as update:
        assert correction._process_fixtures([provider_fixture(SHAKHTAR, LIVYI_BEREH)]) == []
    update.assert_not_called()
