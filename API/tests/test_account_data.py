"""Exercise account routes with separate authenticated users and saved snapshots."""
import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from API import app as api


CARD = {
    "fixture_id": 123, "home": "Home", "away": "Away", "league_id": 61,
    "league": "Ligue 1", "country": "France", "kickoff": "2026-10-20T19:00:00Z",
    "home_crest_url": None, "away_crest_url": None, "league_logo_url": None,
    "pick": {"list": "strength", "favourite": "Home"},
}


class Result:
    def __init__(self, rows): self.rows = rows
    def __iter__(self): return iter(self.rows)
    def mappings(self): return self
    def scalars(self): return self
    def all(self): return self.rows
    def first(self): return self.rows[0] if self.rows else None
    def one(self): assert len(self.rows) == 1; return self.rows[0]


class AccountDB:
    def __init__(self):
        self.users = set()
        self.saved = {}
        self.snapshots = {}
        self.payloads = {}
        self.leagues = set()
        self.statements = []
        self.fail = False
        self.rolled_back = False

    def execute(self, query, params=None):
        sql = " ".join(str(query).split())
        self.statements.append((sql, params))
        if self.fail: raise SQLAlchemyError("private database detail")
        if "SELECT league_id FROM dim.dim_leagues" in sql:
            return Result([league_id for league_id in params["ids"] if league_id in {39, 61, 72}])
        if "SELECT l.league_id" in sql and "dim.dim_leagues" in sql:
            return Result([])
        if "INSERT INTO app.saved_snapshots" in sql:
            if params['digest'] in self.snapshots: return Result([])
            row = dict(params, id=len(self.snapshots)+1, capture_month='2026-10-01')
            self.snapshots[params['digest']] = row
            return Result([row['id']])
        if "FROM app.saved_snapshots WHERE content_hash" in sql:
            return Result([self.snapshots[params['digest']]])
        if "INSERT INTO app.saved_snapshot_payloads" in sql:
            self.payloads[params['id']] = json.loads(params['card'])
            return Result([])
        if "FROM raw.fact_fixtures" in sql or "FROM raw.card_versions" in sql:
            return Result([])
        uid = params["user_id"]
        if "SELECT 1 FROM app.saved_fixture_refs" in sql:
            return Result([1] if (uid, params['fixture_id']) in self.saved else [])
        if "INSERT INTO app.saved_fixture_refs" in sql:
            snapshot = next(row for row in self.snapshots.values() if row['id'] == params['id'])
            self.saved.setdefault((uid, params['fixture_id']), dict(snapshot,
                card=self.payloads[params['id']], saved_at=params['saved_at'] or datetime.now(timezone.utc)))
            return Result([])
        if "FROM app.saved_fixture_refs r JOIN" in sql:
            return Result(sorted([row for (owner, _), row in self.saved.items() if owner == uid], key=lambda row: row['saved_at'], reverse=True))
        if "INSERT INTO app.app_users" in sql:
            self.users.add(uid)
        elif "INSERT INTO app.saved_fixtures" in sql:
            key = uid, params["fixture_id"]
            self.saved.setdefault(key, {
                "card": json.loads(params["card"]), "run_ds": params["run_ds"],
                "saved_at": params["saved_at"] or datetime.now(timezone.utc),
            })
        elif "DELETE FROM app.saved_fixture_refs" in sql:
            self.saved.pop((uid, params["fixture_id"]), None)
        elif "SELECT card, saved_at, run_ds FROM app.saved_fixtures" in sql:
            rows = [row for (owner, _), row in self.saved.items() if owner == uid]
            return Result(sorted(rows, key=lambda row: row["saved_at"], reverse=True))
        elif "DELETE FROM app.user_followed_leagues" in sql:
            self.leagues = {item for item in self.leagues if item[0] != uid or item[1] in params["ids"]}
        elif "INSERT INTO app.user_followed_leagues" in sql:
            self.leagues.add((uid, params["league_id"]))
        elif "SELECT league_id FROM app.user_followed_leagues" in sql:
            return Result(sorted(league for owner, league in self.leagues if owner == uid))
        return Result([])

    def commit(self): pass
    def rollback(self): self.rolled_back = True


@pytest.fixture
def setup():
    db = AccountDB()
    api.app.dependency_overrides[api.get_db] = lambda: db
    yield TestClient(api.app), db
    api.app.dependency_overrides.clear()


def sign_in(user_id):
    api.app.dependency_overrides[api.get_current_user_claims] = lambda: {"sub": user_id}


def test_requires_auth_without_touching_data(setup):
    client, db = setup
    for method, path in [
        ("get", "/account/saved"), ("post", "/account/saved"),
        ("get", "/account/saved/page"), ("get", "/account/saved/ids"),
        ("get", "/account/leagues"), ("put", "/account/leagues"),
    ]:
        assert getattr(client, method)(path).status_code == 401
    assert db.statements == []


def test_saved_snapshot_import_and_user_isolation(setup):
    client, db = setup
    sign_in("alice")
    assert client.get("/account/saved").json() == {"data": []}
    assert db.users == set()  # Reads never create account records.
    response = client.post("/account/saved", json={"card": CARD, "run_ds": "2026-10-18"})
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["data"][0]["card"]["home"] == "Home"
    assert db.users == {"alice"}
    changed = {**CARD, "home": "Changed"}
    assert client.post("/account/saved", json={"card": changed, "run_ds": None}).status_code == 200
    assert client.get("/account/saved").json()["data"][0]["card"]["home"] == "Home"
    second = {**CARD, "fixture_id": 124}
    legacy = {"items": [{"card": second, "run_ds": None, "saved_at": "2026-10-17T10:00:00Z"}]}
    assert client.post("/account/saved/import", json=legacy).status_code == 200
    assert client.post("/account/saved/import", json=legacy).status_code == 200
    assert len(client.get("/account/saved").json()["data"]) == 2
    sign_in("bob")
    assert client.get("/account/saved").json() == {"data": []}
    assert client.delete("/account/saved/123").status_code == 200
    sign_in("alice")
    assert len(client.get("/account/saved").json()["data"]) == 2
    assert client.delete("/account/saved/123").status_code == 200
    assert [item["card"]["fixture_id"] for item in client.get("/account/saved").json()["data"]] == [124]


def test_followed_leagues_replace_and_isolation(setup):
    client, _ = setup
    sign_in("alice")
    assert client.get("/account/leagues").json() == {"data": {"league_ids": []}}
    assert client.put("/account/leagues", json={"league_ids": [61, 39]}).json() == {
        "data": {"league_ids": [39, 61]}}
    sign_in("bob")
    assert client.get("/account/leagues").json() == {"data": {"league_ids": []}}
    assert client.put("/account/leagues", json={"league_ids": [72]}).status_code == 200
    sign_in("alice")
    assert client.put("/account/leagues", json={"league_ids": []}).status_code == 200
    assert client.get("/account/leagues").json() == {"data": {"league_ids": []}}
    sign_in("bob")
    assert client.get("/account/leagues").json() == {"data": {"league_ids": [72]}}


@pytest.mark.parametrize("payload", [
    {}, {"card": CARD, "run_ds": None, "user_id": "bob"},
    {"card": {**CARD, "fixture_id": -1}, "run_ds": None},
    {"card": {**CARD, "home": ""}, "run_ds": None},
])
def test_invalid_saved_payloads_rejected(setup, payload):
    client, db = setup
    sign_in("alice")
    assert client.post("/account/saved", json=payload).status_code == 422
    assert db.saved == {}


@pytest.mark.parametrize("payload", [{}, {"league_ids": [1, 1]}, {"league_ids": [-1]}, {"league_ids": [True]}, {"league_ids": [1], "user_id": "bob"}])
def test_invalid_followed_payloads_rejected(setup, payload):
    client, db = setup
    sign_in("alice")
    assert client.put("/account/leagues", json=payload).status_code == 422
    assert db.leagues == set()


def test_database_errors_are_sanitized_and_rolled_back(setup):
    client, db = setup
    sign_in("alice")
    db.fail = True
    response = client.post("/account/saved", json={"card": CARD, "run_ds": None})
    assert response.status_code == 503
    assert response.json()["detail"] == "Database unavailable"
    assert db.rolled_back
