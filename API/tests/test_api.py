from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import API.app as api_app
from API.app import (
    _load_recent_form_map,
    _avg_goals_from_form_matches,
    _normalize_metric_key,
    _sanitize_recent_form_for_streak,
    app,
    get_current_user_claims,
)
from API.core.database import get_db
from API.utils.headliner import get_event_headline


client = TestClient(app)

UPCOMING_KICKOFF_ISO = "2099-03-07T15:00:00+00:00"
LATER_UPCOMING_KICKOFF_ISO = "2099-03-14T15:00:00+00:00"
RECENT_MATCH_KICKOFF_ISO = "2099-03-01T15:00:00+00:00"


def test_root_returns_list_payload():
    response = client.get("/")
    assert response.status_code in {200, 503}
    if response.status_code == 200:
        payload = response.json()
        assert isinstance(payload, list)
        if payload:
            sample = payload[0]
            assert sample["metric_key"]
            assert sample["headline"]
    else:
        assert response.json()["detail"] == "Database unavailable"


def test_home_streaks_shape():
    response = client.get("/home/streaks")
    assert response.status_code in {200, 503}
    if response.status_code == 200:
        payload = response.json()
        assert isinstance(payload["data"]["streaks"], list)
        assert "generatedAtIso" in payload["meta"]
        assert payload["meta"]["limit"] == 20
        if payload["data"]["streaks"]:
            sample = payload["data"]["streaks"][0]
            assert sample["metric_key"]
            assert sample["headline"]
    else:
        assert response.json()["detail"] == "Database unavailable"


def test_home_streaks_includes_next_is_home_from_team_info():
    from API.models.db_models import DBHomepageStreak, DBTeamInfo

    homepage_rows = [
        SimpleNamespace(
            streak_id="201",
            team_id=11,
            event_name="Win",
            streak_type="result",
            streak_length=5,
            team_name="Arsenal",
            league_name="Premier League",
            next_kickoff_utc=None,
            next_opponent_name="Chelsea",
            next_opponent_id=8,
            next_fixture_id=999,
            context_scope="all",
            team_logo_file_id=None,
            league_logo_file_id=None,
            country_name=None,
            country_logo_file_id=None,
        )
    ]
    team_info_rows = [
        SimpleNamespace(
            team_id=11,
            next_is_home=False,
        )
    ]

    def override_get_db():
        yield _FakeQuerySessionByModel(
            {
                DBHomepageStreak: homepage_rows,
                DBTeamInfo: team_info_rows,
            }
        )

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/home/streaks")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["streaks"][0]["next_is_home"] is False


def test_root_prefers_best_matching_team_info_row_for_enrichment():
    from API.models.db_models import DBNavLeague, DBStreak, DBTeamInfo

    streak_rows = [
        SimpleNamespace(
            streak_id=201,
            team_id=11,
            event_name="Win",
            streak_type="result",
            current_streak_length=5,
            team_name="Arsenal",
            league_name="Premier League",
            next_kickoff_utc=None,
            next_opponent_name="Chelsea",
            next_opponent_id=8,
            next_fixture_id=999,
            context_scope="home",
        )
    ]
    team_info_rows = [
        SimpleNamespace(
            team_id=11,
            league_season_id=3901,
            logo_file_id=None,
            next_is_home=None,
            next_fixture_id=998,
            next_opponent_id=8,
            next_kickoff_utc=None,
        ),
        SimpleNamespace(
            team_id=11,
            league_season_id=3901,
            logo_file_id="arsenal-logo",
            next_is_home=True,
            next_fixture_id=999,
            next_opponent_id=8,
            next_kickoff_utc=UPCOMING_KICKOFF_ISO,
        ),
        SimpleNamespace(
            team_id=8,
            league_season_id=3901,
            logo_file_id="chelsea-logo",
            next_is_home=False,
            next_fixture_id=999,
            next_opponent_id=11,
            next_kickoff_utc=UPCOMING_KICKOFF_ISO,
        ),
    ]
    nav_rows = [
        SimpleNamespace(
            league_season_id=3901,
            league_logo_file_id="league-logo",
            country_logo_file_id="country-logo",
            country_name="England",
        )
    ]

    def override_get_db():
        yield _FakeQuerySessionByModel(
            {
                DBStreak: streak_rows,
                DBTeamInfo: team_info_rows,
                DBNavLeague: nav_rows,
            }
        )

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    sample = response.json()[0]
    assert sample["next_is_home"] is True
    assert sample["team_logo_file_id"] == "arsenal-logo"


def test_root_uses_row_league_season_for_country_when_team_info_missing():
    from API.models.db_models import DBNavLeague, DBStreak, DBTeamInfo

    streak_rows = [
        SimpleNamespace(
            streak_id=608,
            team_id=2525,
            league_season_id=638,
            event_name="winless",
            streak_type="result",
            current_streak_length=19,
            team_name="Chapecoense-sc",
            league_name="Serie A",
            next_kickoff_utc=UPCOMING_KICKOFF_ISO,
            next_opponent_name="Coritiba",
            next_opponent_id=2553,
            next_fixture_id=86412,
            context_scope="all",
        )
    ]
    nav_rows = [
        SimpleNamespace(
            league_season_id=638,
            league_logo_file_id="brazil-serie-a-logo",
            country_logo_file_id="brazil-flag",
            country_name="Brazil",
        )
    ]

    def override_get_db():
        yield _FakeQuerySessionByModel(
            {
                DBStreak: streak_rows,
                DBTeamInfo: [],
                DBNavLeague: nav_rows,
            }
        )

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    sample = response.json()[0]
    assert sample["team_name"] == "Chapecoense-sc"
    assert sample["league_name"] == "Serie A"
    assert sample["country_name"] == "Brazil"
    assert sample["league_logo_file_id"] == "brazil-serie-a-logo"
    assert sample["country_logo_file_id"] == "brazil-flag"


def test_home_streaks_enriches_confidence_recent_form_and_opponent_tier(monkeypatch):
    from API.models.db_models import DBHomepageStreak, DBTeamInfo

    homepage_rows = [
        SimpleNamespace(
            streak_id="301",
            team_id=11,
            league_season_id=3901,
            event_name="Win",
            streak_type="result",
            streak_length=6,
            team_name="Arsenal",
            league_name="Premier League",
            next_kickoff_utc=None,
            next_opponent_name="Chelsea",
            next_opponent_id=8,
            next_fixture_id=1001,
            context_scope="all",
            team_logo_file_id=None,
            league_logo_file_id=None,
            country_name=None,
            country_logo_file_id=None,
        )
    ]
    team_info_rows = [
        SimpleNamespace(team_id=11, league_season_id=3901, next_is_home=True),
        SimpleNamespace(team_id=8, league_season_id=3901, next_is_home=None),
    ]

    monkeypatch.setattr(
        "API.app._load_team_form_score_map",
        lambda *args, **kwargs: {(3901, 11, "win", "all"): 0.8123},
    )
    monkeypatch.setattr(
        "API.app._load_recent_form_by_scope_map",
        lambda *args, **kwargs: {11: {"overall": ["W", "W", "W", "W", "W"], "home": [], "away": []}},
    )
    monkeypatch.setattr(
        "API.app._load_opponent_tier_map",
        lambda *args, **kwargs: {(8, 3901): 3, (11, 3901): 2},
    )

    def override_get_db():
        yield _FakeQuerySessionByModel(
            {
                DBHomepageStreak: homepage_rows,
                DBTeamInfo: team_info_rows,
            }
        )

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/home/streaks")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    sample = response.json()["data"]["streaks"][0]
    assert sample["confidence_score"] == 0.8123
    assert sample["recent_form"] == ["W", "W", "W", "W", "W"]
    assert sample["team_tier"] == 2
    assert sample["opponent_tier"] == 3


def test_home_streaks_prefers_best_matching_team_info_row():
    from API.models.db_models import DBHomepageStreak, DBTeamInfo

    homepage_rows = [
        SimpleNamespace(
            streak_id="301",
            team_id=11,
            league_season_id=3901,
            event_name="Win",
            streak_type="result",
            streak_length=6,
            team_name="Arsenal",
            league_name="Premier League",
            next_kickoff_utc=None,
            next_opponent_name="Chelsea",
            next_opponent_id=8,
            next_fixture_id=1001,
            context_scope="home",
            team_logo_file_id=None,
            league_logo_file_id=None,
            country_name=None,
            country_logo_file_id=None,
        )
    ]
    team_info_rows = [
        SimpleNamespace(
            team_id=11,
            league_season_id=3901,
            logo_file_id=None,
            next_is_home=None,
            next_fixture_id=1000,
            next_opponent_id=8,
            next_kickoff_utc=None,
        ),
        SimpleNamespace(
            team_id=11,
            league_season_id=3901,
            logo_file_id="arsenal-logo",
            next_is_home=True,
            next_fixture_id=1001,
            next_opponent_id=8,
            next_kickoff_utc=UPCOMING_KICKOFF_ISO,
        ),
        SimpleNamespace(
            team_id=8,
            league_season_id=3901,
            logo_file_id="chelsea-logo",
            next_is_home=False,
            next_fixture_id=1001,
            next_opponent_id=11,
            next_kickoff_utc=UPCOMING_KICKOFF_ISO,
        ),
    ]

    def override_get_db():
        yield _FakeQuerySessionByModel(
            {
                DBHomepageStreak: homepage_rows,
                DBTeamInfo: team_info_rows,
            }
        )

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/home/streaks")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    sample = response.json()["data"]["streaks"][0]
    assert sample["next_is_home"] is True


def test_home_stats_shape():
    response = client.get("/home/stats")
    assert response.status_code in {200, 503}
    if response.status_code == 200:
        payload = response.json()
        assert isinstance(payload.get("data"), dict)
        assert isinstance(payload["data"].get("active_streaks"), int)
        assert isinstance(payload["data"].get("leagues"), int)
        assert isinstance(payload["data"].get("teams_tracked"), int)
        assert "generatedAtIso" in payload.get("meta", {})
    else:
        assert response.json()["detail"] == "Database unavailable"


def test_home_stats_returns_counts_from_db(monkeypatch):
    row = {
        "leagues": 10,
        "teams_tracked": 512,
    }
    monkeypatch.setattr("API.app.get_streaks", lambda request, db: [{} for _ in range(3014)])

    def override_get_db():
        yield _FakeExecuteSession([[row]])

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/home/stats")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["active_streaks"] == 3014
    assert payload["data"]["leagues"] == 10
    assert payload["data"]["teams_tracked"] == 512
    assert payload["meta"]["source"] == "raw_mart.mart_top_team_streaks+raw_mart.mart_team_info+raw_mart.mart_league_info"


def test_explore_streaks_stub_shape(monkeypatch):
    sample_rows = [
        {"streak_id": "1", "league_name": "Premier League", "country_name": "England", "streak_type": "goal"},
        {"streak_id": "2", "league_name": "La Liga", "country_name": "Spain", "streak_type": "result"},
        {"streak_id": "3", "league_name": "Premier League", "country_name": "Egypt", "streak_type": "goal"},
    ]

    monkeypatch.setattr("API.app.get_streaks", lambda request, db: sample_rows)

    response = client.get("/explore/streaks", params={"offset": 1, "limit": 1})
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["data"]["streaks"]) == 1
    assert payload["data"]["streaks"][0]["streak_id"] == "2"
    assert payload["data"]["filters"]["leagues"] == ["La Liga", "Premier League"]
    assert payload["data"]["filters"]["types"] == ["goal_streaks", "result_streaks"]
    league_options = payload["data"]["filters"]["league_options"]
    assert len(league_options) == 3
    assert [item["label"] for item in league_options] == [
        "Premier League (Egypt)",
        "Premier League (England)",
        "La Liga (Spain)",
    ]
    assert payload["data"]["filters"]["type_counts"] == {
        "all": 3,
        "goal_streaks": 2,
        "result_streaks": 1,
    }
    assert league_options[0]["goal_streaks_count"] == 1
    assert league_options[0]["result_streaks_count"] == 0
    assert league_options[1]["goal_streaks_count"] == 1
    assert league_options[1]["result_streaks_count"] == 0
    assert league_options[2]["goal_streaks_count"] == 0
    assert league_options[2]["result_streaks_count"] == 1
    assert payload["pagination"]["offset"] == 1
    assert payload["pagination"]["limit"] == 1
    assert payload["pagination"]["nextCursor"] == "2"
    assert payload["pagination"]["hasMore"] is True
    assert payload["meta"]["stub"] is False


def test_explore_streaks_sorts_by_next_match_date_before_pagination(monkeypatch):
    sample_rows = [
        {
            "streak_id": "1",
            "league_name": "Premier League",
            "country_name": "England",
            "streak_type": "goal",
            "team_name": "Later FC",
            "current_streak_length": 8,
            "next_kickoff_utc": "2099-03-20T15:00:00+00:00",
        },
        {
            "streak_id": "2",
            "league_name": "La Liga",
            "country_name": "Spain",
            "streak_type": "result",
            "team_name": "Soon FC",
            "current_streak_length": 3,
            "next_kickoff_utc": "2099-03-10T15:00:00+00:00",
        },
        {
            "streak_id": "3",
            "league_name": "Serie A",
            "country_name": "Italy",
            "streak_type": "goal",
            "team_name": "No Date FC",
            "current_streak_length": 10,
            "next_kickoff_utc": None,
        },
    ]

    monkeypatch.setattr("API.app.get_streaks", lambda request, db: sample_rows)

    response = client.get("/explore/streaks", params={"sort": "match_date_asc", "limit": 2})

    assert response.status_code == 200
    payload = response.json()
    assert [item["streak_id"] for item in payload["data"]["streaks"]] == ["2", "1"]
    assert payload["pagination"]["nextCursor"] == "2"
    assert payload["pagination"]["sort"] == "match_date_asc"
    assert payload["meta"]["sort"] == "match_date_asc"


def test_explore_streaks_stores_redis_cache_on_miss(monkeypatch):
    sample_rows = [
        {
            "streak_id": "1",
            "league_name": "Premier League",
            "country_name": "England",
            "streak_type": "goal",
            "team_logo_file_id": "feedface",
        }
    ]
    fake_redis = _FakeRedis()
    calls = {"count": 0}

    def fake_get_streaks(request, db):
        calls["count"] += 1
        return sample_rows

    monkeypatch.setattr(api_app, "_get_explore_redis_client", lambda: fake_redis)
    monkeypatch.setattr(api_app, "get_streaks", fake_get_streaks)

    response = client.get("/explore/streaks", params={"limit": 1})

    assert response.status_code == 200
    payload = response.json()
    assert calls["count"] == 1
    assert payload["meta"]["cache"]["status"] == "miss_stored"
    assert payload["meta"]["cache"]["store_status"] == "stored"
    assert fake_redis.setex_calls
    cache_key, ttl, raw_value = fake_redis.setex_calls[0]
    assert cache_key == api_app.EXPLORE_STREAKS_CACHE_KEY
    assert ttl == api_app.EXPLORE_STREAKS_CACHE_TTL_SECONDS
    assert json.loads(raw_value)["streaks"][0]["streak_id"] == "1"


def test_explore_streaks_uses_redis_cache_hit(monkeypatch):
    cached_payload = {
        "cachedAtIso": "2099-01-01T00:00:00+00:00",
        "streaks": [
            {
                "streak_id": "cached-1",
                "league_name": "Premier League",
                "country_name": "England",
                "streak_type": "goal",
                "team_logo_file_id": "feedface",
            }
        ],
    }
    fake_redis = _FakeRedis({api_app.EXPLORE_STREAKS_CACHE_KEY: json.dumps(cached_payload)})

    def fail_get_streaks(request, db):
        raise AssertionError("get_streaks should not be called on cache hit")

    monkeypatch.setattr(api_app, "_get_explore_redis_client", lambda: fake_redis)
    monkeypatch.setattr(api_app, "get_streaks", fail_get_streaks)

    response = client.get("/explore/streaks", params={"limit": 1})

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["streaks"][0]["streak_id"] == "cached-1"
    assert payload["data"]["streaks"][0]["team_logo_url"].endswith("/assets/logos/feedface")
    assert payload["meta"]["cache"]["status"] == "hit"
    assert payload["meta"]["cache"]["store_status"] == "skipped"
    assert fake_redis.setex_calls == []


def test_explore_streaks_restore_limit_reads_cached_loaded_slice(monkeypatch):
    cached_payload = {
        "cachedAtIso": "2099-01-01T00:00:00+00:00",
        "streaks": [
            {
                "streak_id": str(idx),
                "league_name": "Premier League",
                "country_name": "England",
                "streak_type": "goal",
                "team_name": f"Team {idx}",
            }
            for idx in range(80)
        ],
    }
    fake_redis = _FakeRedis({api_app.EXPLORE_STREAKS_CACHE_KEY: json.dumps(cached_payload)})

    def fail_get_streaks(request, db):
        raise AssertionError("get_streaks should not be called on restore cache hit")

    monkeypatch.setattr(api_app, "_get_explore_redis_client", lambda: fake_redis)
    monkeypatch.setattr(api_app, "get_streaks", fail_get_streaks)

    response = client.get("/explore/streaks", params={"limit": 50, "restoreLimit": 75})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["data"]["streaks"]) == 75
    assert payload["data"]["streaks"][0]["streak_id"] == "0"
    assert payload["data"]["streaks"][-1]["streak_id"] == "74"
    assert payload["pagination"]["limit"] == 75
    assert payload["pagination"]["nextCursor"] == "75"
    assert payload["pagination"]["total"] == 80
    assert payload["meta"]["cache"]["status"] == "hit"
    assert fake_redis.setex_calls == []


def test_explore_streaks_rejects_invalid_cursor():
    response = client.get("/explore/streaks", params={"cursor": "bad-cursor", "limit": 5})
    assert response.status_code == 422
    assert response.json()["detail"] == "Invalid cursor"


def test_team_stats_requires_team_id():
    response = client.get("/teams/stats")
    assert response.status_code == 422


def test_team_page_context_requires_team_id():
    response = client.get("/pages/team-context")
    assert response.status_code == 422


def test_team_page_data_requires_team_slug():
    response = client.get("/teams/page-data")
    assert response.status_code == 422


def test_fixture_analysis_requires_fixture_id():
    response = client.get("/fixtures/analysis")
    assert response.status_code == 422


def test_fixture_page_context_requires_fixture_id():
    response = client.get("/pages/fixture-context")
    assert response.status_code == 422


def test_metric_key_normalization_maps_known_aliases():
    assert _normalize_metric_key("Win") == "win"
    assert _normalize_metric_key("score 2 goals") == "score_2goals"
    assert _normalize_metric_key("BTTS") == "BTTS"


def test_streak_headline_known_and_unknown_key_behaviour():
    assert get_event_headline("win", mode="streak", streak_length=5, surface="test") == "Won 5 straight matches"
    assert get_event_headline("custom_metric", mode="streak", streak_length=3, surface="test") == "custom metric in 3 straight matches"


def test_sanitize_recent_form_for_streak_conflict_and_valid_cases():
    assert _sanitize_recent_form_for_streak("winless", 5, ["D", "L", "D", "L", "D"]) == ["D", "L", "D", "L", "D"]
    assert _sanitize_recent_form_for_streak("winless", 5, ["D", "W", "D", "L", "D"]) is None
    assert _sanitize_recent_form_for_streak("win", 3, ["W", "W", "W", "D", "L"]) == ["W", "W", "W", "D", "L"]
    assert _sanitize_recent_form_for_streak("win", 3, ["W", "D", "W", "W", "W"]) is None


def test_avg_goals_from_form_matches_parses_scorelines():
    matches = [
        {"score": "3-1"},
        {"score": "1-0"},
        {"score": "2-2"},
    ]
    result = _avg_goals_from_form_matches(matches)
    assert result is not None
    assert result["scored"] == 2.0
    assert result["conceded"] == 1.0


def test_avg_goals_from_form_matches_ignores_invalid_and_returns_none():
    matches = [
        {"score": ""},
        {"score": "postponed"},
        {"score": None},
    ]
    result = _avg_goals_from_form_matches(matches)
    assert result is None


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows

    def query(self, *args, **kwargs):
        return _FakeQuery(self._rows)

    def rollback(self):
        return None


class _FakeExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return self._rows


class _FakeExecuteSession:
    def __init__(self, execute_results):
        self._execute_results = list(execute_results)
        self._idx = 0

    def execute(self, *args, **kwargs):
        rows = self._execute_results[self._idx] if self._idx < len(self._execute_results) else []
        self._idx += 1
        if isinstance(rows, Exception):
            raise rows
        return _FakeExecuteResult(rows)

    def rollback(self):
        return None

    def commit(self):
        return None


class _FakeQuerySessionByModel:
    def __init__(self, rows_by_model):
        self._rows_by_model = rows_by_model

    def query(self, model, *args, **kwargs):
        rows = self._rows_by_model.get(model, [])
        return _FakeQuery(rows)

    def rollback(self):
        return None


class _FakeRedis:
    def __init__(self, initial=None):
        self.values = dict(initial or {})
        self.setex_calls = []

    def get(self, key):
        return self.values.get(key)

    def setex(self, key, ttl, value):
        self.setex_calls.append((key, ttl, value))
        self.values[key] = value
        return True


class _FailingQuerySession:
    def query(self, *args, **kwargs):
        raise SQLAlchemyError("boom")

    def execute(self, *args, **kwargs):
        raise SQLAlchemyError("boom")

    def rollback(self):
        return None


def test_load_recent_form_map_uses_fixtures_scope():
    rows = [
        {"team_id": 11, "is_home": True, "result_value": "win", "kickoff_utc": "2025-01-05", "fixture_id": 5},
        {"team_id": 11, "is_home": False, "result_value": "loss", "kickoff_utc": "2025-01-04", "fixture_id": 4},
        {"team_id": 11, "is_home": True, "result_value": "draw", "kickoff_utc": "2025-01-03", "fixture_id": 3},
        {"team_id": 11, "is_home": False, "result_value": "loss", "kickoff_utc": "2025-01-02", "fixture_id": 2},
        {"team_id": 11, "is_home": True, "result_value": "win", "kickoff_utc": "2025-01-01", "fixture_id": 1},
    ]

    overall = _load_recent_form_map(
        _FakeExecuteSession([rows]),
        team_ids=[11],
        scope_by_team={11: "overall"},
    )
    home = _load_recent_form_map(
        _FakeExecuteSession([rows]),
        team_ids=[11],
        scope_by_team={11: "home"},
    )
    away = _load_recent_form_map(
        _FakeExecuteSession([rows]),
        team_ids=[11],
        scope_by_team={11: "away"},
    )

    assert overall[11] == ["W", "L", "D", "L", "W"]
    assert home[11] == ["W", "D", "W"]
    assert away[11] == ["L", "L"]


def test_root_filters_unsupported_streak_types():
    rows = [
        SimpleNamespace(
            streak_id="101",
            team_id=1,
            event_name="Win",
            streak_type="goal",
            current_streak_length=4,
            team_name="Team A",
            league_name="Premier League",
            next_kickoff_utc=None,
            next_opponent_name=None,
            next_opponent_id=None,
            next_fixture_id=None,
        ),
        SimpleNamespace(
            streak_id="102",
            team_id=2,
            event_name="Special Promo",
            streak_type="special_streaks",
            current_streak_length=9,
            team_name="Team B",
            league_name="Premier League",
            next_kickoff_utc=None,
            next_opponent_name=None,
            next_opponent_id=None,
            next_fixture_id=None,
        ),
    ]

    def override_get_db():
        yield _FakeSession(rows)

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["streak_id"] == "101"
    assert payload[0]["metric_key"] == "win"
    assert payload[0]["headline"]


def test_root_enriches_confidence_recent_form_and_opponent_tier(monkeypatch):
    from API.models.db_models import DBStreak, DBTeamInfo

    streak_rows = [
        SimpleNamespace(
            streak_id="801",
            team_id=11,
            event_name="Win",
            streak_type="result_streaks",
            context_scope="home",
            current_streak_length=5,
            team_name="Arsenal",
            league_name="Premier League",
            next_kickoff_utc=None,
            next_opponent_name="Chelsea",
            next_opponent_id=8,
            next_fixture_id=4001,
        )
    ]
    team_info_rows = [
        SimpleNamespace(team_id=11, league_season_id=3901, next_is_home=True, logo_file_id=None),
        SimpleNamespace(team_id=8, league_season_id=3901, next_is_home=None, logo_file_id=None),
    ]

    monkeypatch.setattr(
        "API.app._load_team_form_score_map",
        lambda *args, **kwargs: {(3901, 11, "win", "home"): 0.7342},
    )
    monkeypatch.setattr(
        "API.app._load_recent_form_by_scope_map",
        lambda *args, **kwargs: {11: {"overall": ["L", "L", "L", "L", "L"], "home": ["W", "W", "W", "W", "W"], "away": []}},
    )
    monkeypatch.setattr(
        "API.app._load_opponent_tier_map",
        lambda *args, **kwargs: {(8, 3901): 2, (11, 3901): 3},
    )

    def override_get_db():
        yield _FakeQuerySessionByModel(
            {
                DBStreak: streak_rows,
                DBTeamInfo: team_info_rows,
            }
        )

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    sample = response.json()[0]
    assert sample["context_scope"] == "home"
    assert sample["confidence_score"] == 0.7342
    assert sample["recent_form"] == ["W", "W", "W", "W", "W"]
    assert sample["team_tier"] == 3
    assert sample["opponent_tier"] == 2


def test_root_filters_context_scope_mismatch_with_next_fixture_venue():
    from API.models.db_models import DBStreak, DBTeamInfo

    streak_rows = [
        SimpleNamespace(
            streak_id="901",
            team_id=11,
            event_name="Score",
            streak_type="goal",
            context_scope="away",
            current_streak_length=8,
            team_name="Bologna",
            league_name="Serie A",
            next_kickoff_utc=None,
            next_opponent_name="Verona",
            next_opponent_id=8,
            next_fixture_id=5001,
        ),
        SimpleNamespace(
            streak_id="902",
            team_id=11,
            event_name="Win",
            streak_type="result",
            context_scope="all",
            current_streak_length=3,
            team_name="Bologna",
            league_name="Serie A",
            next_kickoff_utc=None,
            next_opponent_name="Verona",
            next_opponent_id=8,
            next_fixture_id=5001,
        ),
    ]
    team_info_rows = [SimpleNamespace(team_id=11, league_season_id=3901, next_is_home=True, logo_file_id=None)]

    def override_get_db():
        yield _FakeQuerySessionByModel(
            {
                DBStreak: streak_rows,
                DBTeamInfo: team_info_rows,
            }
        )

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert [item["streak_id"] for item in payload] == ["902"]


def test_root_filters_played_matches_from_explore_feed():
    from API.models.db_models import DBNavLeague, DBStreak, DBTeamInfo

    streak_rows = [
        SimpleNamespace(
            streak_id="950",
            team_id=11,
            event_name="Win",
            streak_type="result",
            context_scope="all",
            current_streak_length=5,
            team_name="Arsenal",
            league_name="Premier League",
            next_kickoff_utc=datetime(2000, 1, 1, tzinfo=timezone.utc),
            next_opponent_name="Chelsea",
            next_opponent_id=8,
            next_fixture_id=5001,
        ),
        SimpleNamespace(
            streak_id="951",
            team_id=12,
            event_name="Under_4_5",
            streak_type="goal",
            context_scope="all",
            current_streak_length=7,
            team_name="Brighton",
            league_name="Premier League",
            next_kickoff_utc=datetime(2099, 1, 1, tzinfo=timezone.utc),
            next_opponent_name="Brentford",
            next_opponent_id=9,
            next_fixture_id=5002,
        ),
    ]
    team_info_rows = [
        SimpleNamespace(team_id=11, league_season_id=3901, next_is_home=True, logo_file_id=None),
        SimpleNamespace(team_id=12, league_season_id=3901, next_is_home=False, logo_file_id=None),
        SimpleNamespace(team_id=8, league_season_id=3901, next_is_home=False, logo_file_id=None),
        SimpleNamespace(team_id=9, league_season_id=3901, next_is_home=True, logo_file_id=None),
    ]
    nav_rows = [
        SimpleNamespace(
            league_season_id=3901,
            league_logo_file_id=None,
            country_logo_file_id=None,
            country_name="England",
        )
    ]

    def override_get_db():
        yield _FakeQuerySessionByModel(
            {
                DBStreak: streak_rows,
                DBTeamInfo: team_info_rows,
                DBNavLeague: nav_rows,
            }
        )

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert [item["streak_id"] for item in payload] == ["951"]


def test_root_enriches_logos_with_internal_team_id_lookup():
    from API.models.db_models import DBNavLeague, DBStreak, DBTeamInfo

    streak_rows = [
        SimpleNamespace(
            streak_id="501",
            team_id=11,
            event_name="Win",
            streak_type="result_streaks",
            current_streak_length=5,
            team_name="Arsenal",
            league_name="Premier League",
            next_kickoff_utc=None,
            next_opponent_name=None,
            next_opponent_id=None,
            next_fixture_id=None,
        )
    ]
    team_info_rows = [
        SimpleNamespace(
            team_id=11,
            team_name="Arsenal",
            team_slug="arsenal-11",
            league_name="Premier League",
            league_season_id=3901,
            logo_file_id="d00dd00d",
        )
    ]
    nav_rows = [
        SimpleNamespace(
            league_season_id=3901,
            league_name="Premier League",
            country_name="England",
            league_logo_file_id="feedface",
            country_logo_file_id="cafebabe",
        )
    ]

    def override_get_db():
        yield _FakeQuerySessionByModel(
            {
                DBStreak: streak_rows,
                DBTeamInfo: team_info_rows,
                DBNavLeague: nav_rows,
            }
        )

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    sample = payload[0]
    assert sample["team_logo_url"] == "http://testserver/assets/logos/d00dd00d"
    assert sample["league_logo_url"] == "http://testserver/assets/logos/feedface"
    assert sample["country_logo_url"] == "http://testserver/assets/logos/cafebabe"
    assert sample["team_logo_file_id"] == "d00dd00d"
    assert sample["league_logo_file_id"] == "feedface"
    assert sample["country_logo_file_id"] == "cafebabe"
    assert sample["country_name"] == "England"


def test_root_returns_503_on_db_error():
    def override_get_db():
        yield _FailingQuerySession()

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 503
    assert response.json()["detail"] == "Database unavailable"


def test_home_streaks_returns_503_on_db_error():
    def override_get_db():
        yield _FailingQuerySession()

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/home/streaks")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 503
    assert response.json()["detail"] == "Database unavailable"


def test_home_stats_returns_503_on_db_error():
    def override_get_db():
        yield _FailingQuerySession()

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/home/stats")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 503
    assert response.json()["detail"] == "Database unavailable"


def test_fixture_page_data_returns_404_when_fixture_missing():
    def override_get_db():
        yield _FakeExecuteSession([[]])

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/fixtures/page-data", params={"fixture_id": 999999})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404
    assert response.json()["detail"] == "Fixture page data not found"


def test_fixture_page_data_tolerates_missing_optional_marts(monkeypatch):
    monkeypatch.setattr("API.app._load_recent_form_by_scope_map", lambda *args, **kwargs: {})

    execute_results = [
        [
            {
                "fixture_id": 45407,
                "league_season_id": 30,
                "league_name": "Championship",
                "kickoff_utc": UPCOMING_KICKOFF_ISO,
                "home_team_id": 422,
                "away_team_id": 427,
                "home_team_name": "Derby",
                "away_team_name": "Sheffield Wednesday",
            }
        ],
        [
            {
                "league_name": "Championship",
                "country_name": "England",
                "league_slug": "championship",
                "league_logo_file_id": "league-logo",
                "country_logo_file_id": "country-logo",
            }
        ],
        [
            {
                "team_id": 422,
                "team_name": "Derby",
                "team_slug": "derby-422",
                "logo_file_id": "derby-logo",
                "venue_id": None,
            },
            {
                "team_id": 427,
                "team_name": "Sheffield Wednesday",
                "team_slug": "sheffield-wednesday-427",
                "logo_file_id": "sw-logo",
                "venue_id": None,
            },
        ],
        [],
        [],
        [],
        SQLAlchemyError("missing mart_fixture_streaks"),
        SQLAlchemyError("missing mart_head_to_head_info"),
        SQLAlchemyError("missing mart_head_to_head_streaks"),
    ]

    def override_get_db():
        yield _FakeExecuteSession(execute_results)

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/fixtures/page-data", params={"fixture_id": 45407})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["fixture"]["fixture_id"] == "45407"
    assert payload["data"]["home_streaks"] == []
    assert payload["data"]["away_streaks"] == []
    assert payload["data"]["h2h_stats"]["streaks"] == []


def test_fixture_page_data_falls_back_to_team_streaks_when_fixture_mart_is_missing(monkeypatch):
    monkeypatch.setattr("API.app._load_recent_form_by_scope_map", lambda *args, **kwargs: {})

    execute_results = [
        [
            {
                "fixture_id": 44893,
                "league_season_id": 24,
                "league_name": "Premier League",
                "kickoff_utc": LATER_UPCOMING_KICKOFF_ISO,
                "home_team_id": 297,
                "away_team_id": 332,
                "home_team_name": "Burnley",
                "away_team_name": "Bournemouth",
            }
        ],
        [
            {
                "league_name": "Premier League",
                "country_name": "England",
                "league_slug": "premier league-24",
                "league_logo_file_id": "league-logo",
                "country_logo_file_id": "country-logo",
            }
        ],
        [
            {
                "team_id": 297,
                "team_name": "Burnley",
                "team_slug": "burnley-297",
                "logo_file_id": "burnley-logo",
                "venue_id": None,
            },
            {
                "team_id": 332,
                "team_name": "Bournemouth",
                "team_slug": "bournemouth-332",
                "logo_file_id": "bournemouth-logo",
                "venue_id": None,
            },
        ],
        [],
        [],
        [],
        SQLAlchemyError("missing mart_fixture_streaks"),
        [
            {
                "team_id": 297,
                "event_name": "concede_1",
                "streak_type": "goal",
                "streak_length": 11,
                "polarity": "negative",
                "scope": "all",
                "fixture_streak_json": {
                    "event_name": "concede_1",
                    "count": 11,
                    "total": None,
                    "scope": "overall",
                    "polarity": "negative",
                },
            }
        ],
        SQLAlchemyError("missing mart_head_to_head_info"),
        SQLAlchemyError("missing mart_head_to_head_streaks"),
    ]

    def override_get_db():
        yield _FakeExecuteSession(execute_results)

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/fixtures/page-data", params={"fixture_id": 44893})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["home_streaks"] == [
        {
            "event": "Conceded in matches",
            "count": 11,
            "total": None,
            "scope": "overall",
            "polarity": "negative",
        }
    ]
    assert payload["data"]["away_streaks"] == []


def test_fixture_page_data_includes_h2h_streaks_from_pair_history(monkeypatch):
    monkeypatch.setattr("API.app._load_recent_form_by_scope_map", lambda *args, **kwargs: {})

    execute_results = [
        [
            {
                "fixture_id": 45407,
                "league_season_id": 30,
                "league_name": "Championship",
                "kickoff_utc": UPCOMING_KICKOFF_ISO,
                "home_team_id": 422,
                "away_team_id": 427,
                "home_team_name": "Derby",
                "away_team_name": "Sheffield Wednesday",
            }
        ],
        [
            {
                "league_name": "Championship",
                "country_name": "England",
                "league_slug": "championship",
                "league_logo_file_id": "league-logo",
                "country_logo_file_id": "country-logo",
            }
        ],
        [
            {
                "team_id": 422,
                "team_name": "Derby",
                "team_slug": "derby-422",
                "logo_file_id": "derby-logo",
                "venue_id": None,
            },
            {
                "team_id": 427,
                "team_name": "Sheffield Wednesday",
                "team_slug": "sheffield-wednesday-427",
                "logo_file_id": "sw-logo",
                "venue_id": None,
            },
        ],
        [
            {
                "team_id": 422,
                "team_name": "Derby",
                "avg_goals_for": 1.4,
                "avg_goals_against": 1.1,
                "recent_form": "W,W,D,L,W",
                "next_fixture_id": 45407,
                "kickoff_utc": RECENT_MATCH_KICKOFF_ISO,
                "games_played": 10,
            },
            {
                "team_id": 427,
                "team_name": "Sheffield Wednesday",
                "avg_goals_for": 0.9,
                "avg_goals_against": 1.8,
                "recent_form": "L,L,D,L,L",
                "next_fixture_id": 45407,
                "kickoff_utc": RECENT_MATCH_KICKOFF_ISO,
                "games_played": 10,
            },
        ],
        [
            {
                "team_id": 422,
                "overall_scored": 1.4,
                "overall_conceded": 1.1,
                "home_scored": 1.6,
                "home_conceded": 0.8,
                "away_scored": 1.2,
                "away_conceded": 1.4,
            },
            {
                "team_id": 427,
                "overall_scored": 0.9,
                "overall_conceded": 1.8,
                "home_scored": 1.0,
                "home_conceded": 1.5,
                "away_scored": 0.8,
                "away_conceded": 2.1,
            },
        ],
        [],
        [],
        [
            {
                "h2h_info_json": {"totalMatches": 5, "homeWins": 1, "draws": 1, "awayWins": 3},
                "h2h_streaks_json": [
                    {
                        "event_name": "Over_1_5",
                        "streak_length": 4,
                        "total": 4,
                        "teams": "both",
                        "fixture_id": 45230,
                        "team_id": 422,
                    }
                ],
                "h2h_matches_json": [
                    {
                        "date": "2025-12-15",
                        "home": 0,
                        "away": 3,
                        "result": "A",
                        "isHomeTeamHome": False,
                    }
                ],
            }
        ],
        [],
    ]

    def override_get_db():
        yield _FakeExecuteSession(execute_results)

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/fixtures/page-data", params={"fixture_id": 45407})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["h2h_stats"]["recent_matches"][0]["result"] == "A"
    assert payload["data"]["h2h_stats"]["streaks"] == [
        {"event": "Over 1.5 goals", "count": 4, "total": 4, "teams": "both"}
    ]


def test_fixture_page_data_excludes_partial_h2h_trends_when_count_is_less_than_total(monkeypatch):
    monkeypatch.setattr("API.app._load_recent_form_by_scope_map", lambda *args, **kwargs: {})

    execute_results = [
        [
            {
                "fixture_id": 45407,
                "league_season_id": 30,
                "league_name": "Championship",
                "kickoff_utc": UPCOMING_KICKOFF_ISO,
                "home_team_id": 422,
                "away_team_id": 427,
                "home_team_name": "Derby",
                "away_team_name": "Sheffield Wednesday",
            }
        ],
        [
            {
                "league_name": "Championship",
                "country_name": "England",
                "league_slug": "championship",
                "league_logo_file_id": "league-logo",
                "country_logo_file_id": "country-logo",
            }
        ],
        [
            {
                "team_id": 422,
                "team_name": "Derby",
                "team_slug": "derby-422",
                "logo_file_id": "derby-logo",
                "venue_id": None,
            },
            {
                "team_id": 427,
                "team_name": "Sheffield Wednesday",
                "team_slug": "sheffield-wednesday-427",
                "logo_file_id": "sw-logo",
                "venue_id": None,
            },
        ],
        [
            {
                "team_id": 422,
                "team_name": "Derby",
                "avg_goals_for": 1.4,
                "avg_goals_against": 1.1,
                "recent_form": "W,W,D,L,W",
                "next_fixture_id": 45407,
                "kickoff_utc": RECENT_MATCH_KICKOFF_ISO,
                "games_played": 10,
            },
            {
                "team_id": 427,
                "team_name": "Sheffield Wednesday",
                "avg_goals_for": 0.9,
                "avg_goals_against": 1.8,
                "recent_form": "L,L,D,L,L",
                "next_fixture_id": 45407,
                "kickoff_utc": RECENT_MATCH_KICKOFF_ISO,
                "games_played": 10,
            },
        ],
        [
            {
                "team_id": 422,
                "overall_scored": 1.4,
                "overall_conceded": 1.1,
                "home_scored": 1.6,
                "home_conceded": 0.8,
                "away_scored": 1.2,
                "away_conceded": 1.4,
            },
            {
                "team_id": 427,
                "overall_scored": 0.9,
                "overall_conceded": 1.8,
                "home_scored": 1.0,
                "home_conceded": 1.5,
                "away_scored": 0.8,
                "away_conceded": 2.1,
            },
        ],
        [],
        [],
        [
            {
                "h2h_info_json": {"totalMatches": 11, "homeWins": 4, "draws": 2, "awayWins": 5},
                "h2h_streaks_json": [
                    {
                        "event_name": "Over_1_5",
                        "streak_length": 4,
                        "total": 4,
                        "teams": "both",
                        "fixture_id": 45230,
                        "team_id": 422,
                    },
                    {
                        "event_name": "BTTS",
                        "streak_length": 4,
                        "total": 7,
                        "teams": "both",
                        "fixture_id": 45230,
                        "team_id": 422,
                    },
                ],
                "h2h_matches_json": [],
            }
        ],
        [],
    ]

    def override_get_db():
        yield _FakeExecuteSession(execute_results)

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/fixtures/page-data", params={"fixture_id": 45407})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["h2h_stats"]["streaks"] == [
        {"event": "Over 1.5 goals", "count": 4, "total": 4, "teams": "both"}
    ]


def test_fixture_page_data_maps_h2h_streak_ownership_to_team_when_not_shared(monkeypatch):
    monkeypatch.setattr("API.app._load_recent_form_by_scope_map", lambda *args, **kwargs: {})

    execute_results = [
        [
            {
                "fixture_id": 45407,
                "league_season_id": 30,
                "league_name": "Championship",
                "kickoff_utc": UPCOMING_KICKOFF_ISO,
                "home_team_id": 422,
                "away_team_id": 427,
                "home_team_name": "Derby",
                "away_team_name": "Sheffield Wednesday",
            }
        ],
        [
            {
                "league_name": "Championship",
                "country_name": "England",
                "league_slug": "championship",
                "league_logo_file_id": "league-logo",
                "country_logo_file_id": "country-logo",
            }
        ],
        [
            {
                "team_id": 422,
                "team_name": "Derby",
                "team_slug": "derby-422",
                "logo_file_id": "derby-logo",
                "venue_id": None,
            },
            {
                "team_id": 427,
                "team_name": "Sheffield Wednesday",
                "team_slug": "sheffield-wednesday-427",
                "logo_file_id": "sw-logo",
                "venue_id": None,
            },
        ],
        [
            {
                "team_id": 422,
                "team_name": "Derby",
                "avg_goals_for": 1.4,
                "avg_goals_against": 1.1,
                "recent_form": "W,W,D,L,W",
                "next_fixture_id": 45407,
                "kickoff_utc": RECENT_MATCH_KICKOFF_ISO,
                "games_played": 10,
            },
            {
                "team_id": 427,
                "team_name": "Sheffield Wednesday",
                "avg_goals_for": 0.9,
                "avg_goals_against": 1.8,
                "recent_form": "L,L,D,L,L",
                "next_fixture_id": 45407,
                "kickoff_utc": RECENT_MATCH_KICKOFF_ISO,
                "games_played": 10,
            },
        ],
        [
            {
                "team_id": 422,
                "overall_scored": 1.4,
                "overall_conceded": 1.1,
                "home_scored": 1.6,
                "home_conceded": 0.8,
                "away_scored": 1.2,
                "away_conceded": 1.4,
            },
            {
                "team_id": 427,
                "overall_scored": 0.9,
                "overall_conceded": 1.8,
                "home_scored": 1.0,
                "home_conceded": 1.5,
                "away_scored": 0.8,
                "away_conceded": 2.1,
            },
        ],
        [],
        [],
        [
            {
                "h2h_info_json": {"totalMatches": 5, "homeWins": 1, "draws": 1, "awayWins": 3},
                "h2h_streaks_json": [
                    {
                        "event_name": "winless",
                        "streak_length": 4,
                        "total": 4,
                        "teams": "all",
                        "fixture_id": 45230,
                        "team_id": 427,
                    },
                    {
                        "event_name": "BTTS",
                        "streak_length": 3,
                        "total": 3,
                        "teams": "all",
                        "fixture_id": 45230,
                        "team_id": 422,
                    },
                    {
                        "event_name": "concede_1",
                        "streak_length": 2,
                        "total": 4,
                        "scope": "home",
                        "teams": "all",
                        "fixture_id": 45230,
                        "team_id": 422,
                    },
                ],
                "h2h_matches_json": [],
            }
        ],
        [],
    ]

    def override_get_db():
        yield _FakeExecuteSession(execute_results)

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/fixtures/page-data", params={"fixture_id": 45407})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["h2h_stats"]["streaks"] == [
        {"event": "Winless matches", "count": 4, "total": 4, "teams": "away"},
        {"event": "Both teams scored", "count": 3, "total": 3, "teams": "both"},
    ]


def test_fixture_page_data_prefers_team_id_over_historical_h2h_teams_label(monkeypatch):
    monkeypatch.setattr("API.app._load_recent_form_by_scope_map", lambda *args, **kwargs: {})

    execute_results = [
        [
            {
                "fixture_id": 45407,
                "league_season_id": 30,
                "league_name": "Championship",
                "kickoff_utc": UPCOMING_KICKOFF_ISO,
                "home_team_id": 422,
                "away_team_id": 427,
                "home_team_name": "Derby",
                "away_team_name": "Sheffield Wednesday",
            }
        ],
        [
            {
                "league_name": "Championship",
                "country_name": "England",
                "league_slug": "championship",
                "league_logo_file_id": "league-logo",
                "country_logo_file_id": "country-logo",
            }
        ],
        [
            {
                "team_id": 422,
                "team_name": "Derby",
                "team_slug": "derby-422",
                "logo_file_id": "derby-logo",
                "venue_id": None,
            },
            {
                "team_id": 427,
                "team_name": "Sheffield Wednesday",
                "team_slug": "sheffield-wednesday-427",
                "logo_file_id": "sw-logo",
                "venue_id": None,
            },
        ],
        [
            {
                "team_id": 422,
                "team_name": "Derby",
                "avg_goals_for": 1.4,
                "avg_goals_against": 1.1,
                "recent_form": "W,W,D,L,W",
                "next_fixture_id": 45407,
                "kickoff_utc": RECENT_MATCH_KICKOFF_ISO,
                "games_played": 10,
            },
            {
                "team_id": 427,
                "team_name": "Sheffield Wednesday",
                "avg_goals_for": 0.9,
                "avg_goals_against": 1.8,
                "recent_form": "L,L,D,L,L",
                "next_fixture_id": 45407,
                "kickoff_utc": RECENT_MATCH_KICKOFF_ISO,
                "games_played": 10,
            },
        ],
        [
            {
                "team_id": 422,
                "overall_scored": 1.4,
                "overall_conceded": 1.1,
                "home_scored": 1.6,
                "home_conceded": 0.8,
                "away_scored": 1.2,
                "away_conceded": 1.4,
            },
            {
                "team_id": 427,
                "overall_scored": 0.9,
                "overall_conceded": 1.8,
                "home_scored": 1.0,
                "home_conceded": 1.5,
                "away_scored": 0.8,
                "away_conceded": 2.1,
            },
        ],
        [],
        [],
        [
            {
                "h2h_info_json": {"totalMatches": 5, "homeWins": 1, "draws": 1, "awayWins": 3},
                "h2h_streaks_json": [
                    {
                        "event_name": "score_1goal",
                        "streak_length": 4,
                        "total": 4,
                        "teams": "home",
                        "fixture_id": 45230,
                        "team_id": 427,
                    }
                ],
                "h2h_matches_json": [],
            }
        ],
        [],
    ]

    def override_get_db():
        yield _FakeExecuteSession(execute_results)

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/fixtures/page-data", params={"fixture_id": 45407})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["h2h_stats"]["streaks"] == [
        {"event": "Scored in matches", "count": 4, "total": 4, "teams": "away"}
    ]


def test_fixture_page_data_ignores_non_overall_h2h_streak_scope(monkeypatch):
    monkeypatch.setattr("API.app._load_recent_form_by_scope_map", lambda *args, **kwargs: {})

    execute_results = [
        [
            {
                "fixture_id": 45407,
                "league_season_id": 30,
                "league_name": "Championship",
                "kickoff_utc": UPCOMING_KICKOFF_ISO,
                "home_team_id": 422,
                "away_team_id": 427,
                "home_team_name": "Derby",
                "away_team_name": "Sheffield Wednesday",
            }
        ],
        [
            {
                "league_name": "Championship",
                "country_name": "England",
                "league_slug": "championship",
                "league_logo_file_id": "league-logo",
                "country_logo_file_id": "country-logo",
            }
        ],
        [
            {
                "team_id": 422,
                "team_name": "Derby",
                "team_slug": "derby-422",
                "logo_file_id": "derby-logo",
                "venue_id": None,
            },
            {
                "team_id": 427,
                "team_name": "Sheffield Wednesday",
                "team_slug": "sheffield-wednesday-427",
                "logo_file_id": "sw-logo",
                "venue_id": None,
            },
        ],
        [
            {
                "team_id": 422,
                "team_name": "Derby",
                "avg_goals_for": 1.4,
                "avg_goals_against": 1.1,
                "recent_form": "W,W,D,L,W",
                "next_fixture_id": 45407,
                "kickoff_utc": RECENT_MATCH_KICKOFF_ISO,
                "games_played": 10,
            },
            {
                "team_id": 427,
                "team_name": "Sheffield Wednesday",
                "avg_goals_for": 0.9,
                "avg_goals_against": 1.8,
                "recent_form": "L,L,D,L,L",
                "next_fixture_id": 45407,
                "kickoff_utc": RECENT_MATCH_KICKOFF_ISO,
                "games_played": 10,
            },
        ],
        [
            {
                "team_id": 422,
                "overall_scored": 1.4,
                "overall_conceded": 1.1,
                "home_scored": 1.6,
                "home_conceded": 0.8,
                "away_scored": 1.2,
                "away_conceded": 1.4,
            },
            {
                "team_id": 427,
                "overall_scored": 0.9,
                "overall_conceded": 1.8,
                "home_scored": 1.0,
                "home_conceded": 1.5,
                "away_scored": 0.8,
                "away_conceded": 2.1,
            },
        ],
        [],
        [],
        [
            {
                "h2h_info_json": {"totalMatches": 5, "homeWins": 1, "draws": 1, "awayWins": 3},
                "h2h_streaks_json": [
                    {
                        "event_name": "score_1goal",
                        "streak_length": 4,
                        "total": 4,
                        "scope": "home",
                        "teams": "home",
                        "fixture_id": 45230,
                        "team_id": 422,
                    },
                    {
                        "event_name": "Under_3_5",
                        "streak_length": 3,
                        "total": 3,
                        "scope": "all",
                        "teams": "both",
                        "fixture_id": 45230,
                        "team_id": 422,
                    },
                ],
                "h2h_matches_json": [],
            }
        ],
        [],
    ]

    def override_get_db():
        yield _FakeExecuteSession(execute_results)

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/fixtures/page-data", params={"fixture_id": 45407})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["h2h_stats"]["streaks"] == [
        {"event": "Under 3.5 goals", "count": 3, "total": 3, "teams": "both"}
    ]


def test_team_page_data_returns_404_when_team_missing():
    def override_get_db():
        yield _FakeExecuteSession([[], []])

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/teams/page-data", params={"team_slug": "missing-team-999999"})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404
    assert response.json()["detail"] == "Team page data not found"


def test_navigation_leagues_shape_dedupe_and_fallback_slug():
    from API.models.db_models import DBLeagueInfo, DBNavLeague

    nav_rows = [
        SimpleNamespace(
            league_season_id=100,
            league_name="Premier League",
            country_name="England",
            league_logo_file_id="deadbeef",
            country_logo_file_id="cafebabe",
        ),
        SimpleNamespace(
            league_season_id=101,
            league_name="Premier League",
            country_name="England",
            league_logo_file_id="feedface",
            country_logo_file_id="cafebabe",
        ),
        SimpleNamespace(
            league_season_id=200,
            league_name="La Liga",
            country_name="Spain",
            league_logo_file_id=None,
            country_logo_file_id=None,
        ),
    ]
    league_info_rows = [
        SimpleNamespace(
            league_season_id=100,
            league_name="Premier League",
            league_slug="premier-league",
            season_label="2023/24",
            country_name="England",
        ),
        SimpleNamespace(
            league_season_id=101,
            league_name="Premier League",
            league_slug="premier-league",
            season_label="2024/25",
            country_name="England",
        ),
        SimpleNamespace(
            league_season_id=200,
            league_name="La Liga",
            league_slug=None,
            season_label="2024/25",
            country_name="Spain",
        ),
    ]

    def override_get_db():
        yield _FakeQuerySessionByModel(
            {
                DBNavLeague: nav_rows,
                DBLeagueInfo: league_info_rows,
            }
        )

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/navigation/leagues")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert "generatedAtIso" in payload["meta"]
    assert payload["meta"]["source"] == "raw_mart.mart_nav_leagues+mart_league_info"
    assert payload["meta"]["deduped"] is True
    assert payload["meta"]["fallbackSlugCount"] == 1
    assert isinstance(payload["data"]["leagues"], list)
    assert len(payload["data"]["leagues"]) == 2

    premier = next(item for item in payload["data"]["leagues"] if item["league_slug"] == "premier-league")
    assert premier["league_season_id"] == 101
    assert premier["href"] == "/league/premier-league"
    assert premier["league_logo_url"] == "http://testserver/assets/logos/feedface"
    assert premier["country_logo_url"] == "http://testserver/assets/logos/cafebabe"

    la_liga = next(item for item in payload["data"]["leagues"] if item["league_name"] == "La Liga")
    assert la_liga["league_slug"] == "la-liga"
    assert la_liga["league_logo_url"] is None
    assert la_liga["country_logo_url"] is None


def test_navigation_leagues_returns_503_on_db_error():
    def override_get_db():
        yield _FailingQuerySession()

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/navigation/leagues")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 503
    assert response.json()["detail"] == "Database unavailable"


def test_search_teams_returns_suggestions_and_team_links():
    rows = [
        {
            "team_id": 112,
            "name": "Anderlecht",
            "team_name": "Anderlecht",
            "team_slug": "anderlecht-112",
            "logo_file_id": "dcd82036-b75a-438f-b533-01802634d8c9",
            "league_name": "Jupiler Pro League",
            "country_logo_file_id": "d72f794b-69e0-4250-8ca2-edf806c6f8ae",
        },
        {
            "team_id": 113,
            "name": "Club Brugge Kv",
            "team_name": "Club Brugge Kv",
            "team_slug": "club brugge kv-113",
            "logo_file_id": None,
            "league_name": "Jupiler Pro League",
            "country_logo_file_id": None,
        },
    ]

    def override_get_db():
        yield _FakeExecuteSession([rows])

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/search/teams", params={"q": "and", "limit": 5})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["query"] == "and"
    assert payload["meta"]["limit"] == 5
    assert payload["meta"]["source"] == "raw_mart.mart_dim_search"
    assert len(payload["data"]["suggestions"]) == 2
    first = payload["data"]["suggestions"][0]
    assert first["team_id"] == 112
    assert first["slug"] == "anderlecht-112"
    assert first["href"] == "/team/anderlecht-112"
    assert first["logo_url"] == "http://testserver/assets/logos/dcd82036-b75a-438f-b533-01802634d8c9"
    assert first["league_name"] == "Jupiler Pro League"
    assert first["country_logo_url"] == "http://testserver/assets/logos/d72f794b-69e0-4250-8ca2-edf806c6f8ae"


def test_search_teams_ignores_legacy_logo_uuid():
    rows = [
        {
            "team_id": 112,
            "name": "Anderlecht",
            "team_name": "Anderlecht",
            "team_slug": "anderlecht-112",
            "logo_uuid": "dcd82036-b75a-438f-b533-01802634d8c9",
            "league_name": "Jupiler Pro League",
            "country_logo_file_id": "d72f794b-69e0-4250-8ca2-edf806c6f8ae",
        },
    ]

    def override_get_db():
        yield _FakeExecuteSession([rows])

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/search/teams", params={"q": "and", "limit": 5})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["suggestions"][0]["logo_url"] is None


def test_search_teams_returns_503_on_db_error():
    def override_get_db():
        yield _FailingQuerySession()

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/search/teams", params={"q": "ars"})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 503
    assert response.json()["detail"] == "Database unavailable"


def test_league_page_data_returns_shape():
    from API.models.db_models import DBNavLeague, DBStreak, DBTeamInfo

    league_row = {
        "league_season_id": 430,
        "league_name": "Premier League",
        "league_slug": "premier-league-430",
        "season_label": "2025",
        "country_name": "England",
        "league_logo_file_id": "feedface",
        "country_logo_file_id": "cafebabe",
    }
    team_rows = [
        {
            "team_id": 1,
            "team_name": "Arsenal",
            "team_slug": "arsenal-1",
            "logo_file_id": "d00dd00d",
            "rank": 1,
            "points": 60,
            "games_played": 24,
            "wins": 19,
            "draws": 3,
            "losses": 2,
            "goals_for": 58,
            "goals_against": 22,
            "goal_diff": 36,
            "form": "WWDLW",
        },
        {
            "team_id": 2,
            "team_name": "Chelsea",
            "team_slug": "chelsea-2",
            "logo_file_id": None,
            "rank": 2,
            "points": 55,
            "games_played": 24,
            "wins": 17,
            "draws": 4,
            "losses": 3,
            "goals_for": 49,
            "goals_against": 25,
            "goal_diff": 24,
            "form": "WDLDW",
        },
    ]
    streak_rows = [
        SimpleNamespace(
            streak_id=91,
            team_id=1,
            league_season_id=430,
            league_name="Premier League",
            team_name="Arsenal",
            next_opponent_name="Chelsea",
            next_opponent_id=2,
            next_fixture_id=12345,
            next_kickoff_utc=datetime(2099, 3, 10, 19, 45, tzinfo=timezone.utc),
            event_name="win",
            streak_type="result",
            context_scope="overall",
            current_streak_length=5,
        )
    ]
    team_info_rows = [
        SimpleNamespace(team_id=1, league_season_id=430, logo_file_id="d00dd00d", next_is_home=True, next_kickoff_utc=datetime(2099, 3, 10, 19, 45, tzinfo=timezone.utc)),
        SimpleNamespace(team_id=2, league_season_id=430, logo_file_id="opp-logo", next_is_home=False, next_kickoff_utc=datetime(2099, 3, 10, 19, 45, tzinfo=timezone.utc)),
    ]
    nav_rows = [
        SimpleNamespace(league_season_id=430, league_logo_file_id="feedface", country_logo_file_id="cafebabe", country_name="England")
    ]

    def override_get_db():
        class _FakeLeaguePageSession(_FakeExecuteSession):
            def __init__(self):
                super().__init__([[league_row], team_rows])
                self._rows_by_model = {
                    DBStreak: streak_rows,
                    DBTeamInfo: team_info_rows,
                    DBNavLeague: nav_rows,
                }

            def query(self, model, *args, **kwargs):
                return _FakeQuery(self._rows_by_model.get(model, []))

        fake_db = _FakeLeaguePageSession()
        yield fake_db

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/leagues/page-data", params={"league_slug": "premier-league-430"})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["league"]["league_slug"] == "premier-league-430"
    assert payload["data"]["league"]["league_logo_url"] == "http://testserver/assets/logos/feedface"
    assert payload["data"]["top_streaks"][0]["team_name"] == "Arsenal"
    assert payload["data"]["top_streaks"][0]["league_logo_url"] == "http://testserver/assets/logos/feedface"
    assert payload["data"]["top_streaks"][0]["next_fixture_id"] == 12345
    assert payload["data"]["standings"]["overall"][0]["team_slug"] == "arsenal-1"
    assert payload["data"]["standings"]["overall"][0]["form"] == ["W", "W", "D", "L", "W"]
    assert payload["data"]["standings"]["overall"][0]["played"] == 24
    assert payload["data"]["standings"]["overall"][0]["wins"] == 19
    assert payload["data"]["standings"]["overall"][0]["draws"] == 3
    assert payload["data"]["standings"]["overall"][0]["losses"] == 2
    assert payload["data"]["standings"]["overall"][0]["goals_for"] == 58
    assert payload["data"]["standings"]["overall"][0]["goals_against"] == 22
    assert payload["data"]["standings"]["overall"][0]["goal_difference"] == 36
    assert payload["data"]["available_contexts"] == ["overall"]


def test_league_page_data_falls_back_to_fixture_teams_when_standings_are_missing():
    from API.models.db_models import DBStreak

    league_row = {
        "league_season_id": 590,
        "league_name": "League Two",
        "league_slug": "league two-590",
        "season_label": "2026",
        "country_name": "China",
        "league_logo_file_id": "league-logo",
        "country_logo_file_id": "country-logo",
    }
    fallback_team_rows = [
        {
            "team_id": 9001,
            "team_name": "Yunnan Yukun",
            "team_slug": "yunnan-yukun-9001",
            "logo_file_id": "team-logo",
            "rank": None,
            "points": None,
            "games_played": None,
            "wins": None,
            "draws": None,
            "losses": None,
            "goals_for": None,
            "goals_against": None,
            "goal_diff": None,
            "form": None,
        }
    ]

    class _FakeLeaguePageSession:
        def __init__(self):
            self._execute_results = [
                [league_row],
                [],
                fallback_team_rows,
            ]
            self._idx = 0

        def execute(self, *args, **kwargs):
            rows = self._execute_results[self._idx] if self._idx < len(self._execute_results) else []
            self._idx += 1
            return _FakeExecuteResult(rows)

        def query(self, model, *args, **kwargs):
            return _FakeQuery([])

        def rollback(self):
            return None

    def override_get_db():
        yield _FakeLeaguePageSession()

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/leagues/page-data", params={"league_slug": "league two-590"})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["league"]["league_name"] == "League Two"
    assert payload["data"]["league"]["league_logo_url"] == "http://testserver/assets/logos/league-logo"
    assert payload["data"]["standings"]["overall"][0]["team_name"] == "Yunnan Yukun"
    assert payload["data"]["standings"]["overall"][0]["team_logo_url"] == "http://testserver/assets/logos/team-logo"
    assert payload["data"]["standings"]["overall"][0]["points"] is None
    assert payload["data"]["standings"]["overall"][0]["form"] == []


def test_league_page_data_returns_404_when_missing():
    def override_get_db():
        yield _FakeExecuteSession([[], []])

    app.dependency_overrides[get_db] = override_get_db
    try:
        response = client.get("/leagues/page-data", params={"league_slug": "missing-league"})
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404
    assert response.json()["detail"] == "League page data not found"


def test_me_returns_synced_user_payload():
    user_row = {
        "user_id": "user_123",
        "email": "test@example.com",
        "first_name": "Test",
        "last_name": "User",
        "image_url": "https://example.com/u.png",
        "plan": "free",
        "onboarding_completed": False,
    }

    def override_get_db():
        # CREATE SCHEMA, CREATE TABLE, UPSERT RETURNING
        yield _FakeExecuteSession([[], [], [user_row]])

    def override_claims():
        return {
            "sub": "user_123",
            "email": "test@example.com",
            "first_name": "Test",
            "last_name": "User",
            "image_url": "https://example.com/u.png",
        }

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_current_user_claims] = override_claims
    try:
        response = client.get("/me")
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_current_user_claims, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["userId"] == "user_123"
    assert payload["data"]["email"] == "test@example.com"
    assert payload["data"]["plan"] == "free"
    assert payload["data"]["onboardingCompleted"] is False
    assert payload["meta"]["authProvider"] == "clerk"
    assert payload["meta"]["source"] == "app.app_users"


def test_me_returns_503_on_db_error():
    def override_get_db():
        yield _FailingQuerySession()

    def override_claims():
        return {"sub": "user_123"}

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_current_user_claims] = override_claims
    try:
        response = client.get("/me")
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_current_user_claims, None)

    assert response.status_code == 503
    assert response.json()["detail"] == "Database unavailable"


def test_me_hydrates_missing_email_from_clerk_profile():
    user_row = {
        "user_id": "user_456",
        "email": "hydrated@example.com",
        "first_name": "Hydrated",
        "last_name": "User",
        "image_url": "https://example.com/hydrated.png",
        "plan": "free",
        "onboarding_completed": False,
    }

    def override_get_db():
        # CREATE SCHEMA, CREATE TABLE, UPSERT RETURNING
        yield _FakeExecuteSession([[], [], [user_row]])

    def override_claims():
        return {"sub": "user_456"}

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_current_user_claims] = override_claims
    try:
        with patch(
            "API.app._fetch_clerk_user_profile",
            return_value={
                "email": "hydrated@example.com",
                "first_name": "Hydrated",
                "last_name": "User",
                "image_url": "https://example.com/hydrated.png",
            },
        ):
            response = client.get("/me")
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_current_user_claims, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["userId"] == "user_456"
    assert payload["data"]["email"] == "hydrated@example.com"
