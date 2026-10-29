import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError
from API import app as api


class DB:
    def __init__(self): self.statements = []
    def execute(self, query, params): self.statements.append((str(query), params))
    def commit(self): pass
    def rollback(self): self.rolled_back = True


@pytest.fixture
def setup(monkeypatch):
    db = DB()
    store = {}
    api.app.dependency_overrides[api.get_db] = lambda: db
    monkeypatch.setattr(api, '_upsert_app_user_from_claims', lambda db, claims: None)
    monkeypatch.setattr(api, 'read_preferences', lambda db, uid: {'timezone_override': store.get(uid)})
    def save(db, uid, zone):
        store[uid] = zone
        return {'timezone_override': zone}
    monkeypatch.setattr(api, 'save_preferences', save)
    yield TestClient(api.app), db, store
    api.app.dependency_overrides.clear()


def signin(uid):
    api.app.dependency_overrides[api.get_current_user_claims] = lambda: {'sub': uid}


def test_requires_auth(setup):
    client, _, _ = setup
    assert client.get('/preferences').status_code == 401
    assert client.post('/preferences', json={'timezone_override': None}).status_code == 401


def test_roundtrip_reset_and_isolation(setup):
    client, _, store = setup
    signin('alice')
    assert client.get('/preferences').json()['data']['timezone_override'] is None
    assert client.post('/preferences', json={'timezone_override': 'Europe/London'}).status_code == 200
    assert client.get('/preferences').json()['data']['timezone_override'] == 'Europe/London'
    signin('bob')
    assert client.get('/preferences').json()['data']['timezone_override'] is None
    assert client.post('/preferences', json={'timezone_override': 'Asia/Tokyo'}).status_code == 200
    signin('alice')
    assert client.post('/preferences', json={'timezone_override': None}).status_code == 200
    assert store == {'alice': None, 'bob': 'Asia/Tokyo'}


@pytest.mark.parametrize('body', [{}, {'timezone_override': ''}, {'timezone_override': 'Mars/Olympus'}, {'timezone_override': '../etc/passwd'}, {'timezone_override': 123}, {'timezone_override': None, 'user_id': 'someone_else'}])
def test_invalid_updates(setup, body):
    client, _, store = setup
    signin('alice')
    assert client.post('/preferences', json=body).status_code == 422
    assert not store


def test_db_error_rolls_back(setup, monkeypatch):
    client, db, _ = setup
    signin('alice')
    def fail(*args): raise SQLAlchemyError('private connection information')
    monkeypatch.setattr(api, 'save_preferences', fail)
    response = client.post('/preferences', json={'timezone_override': None})
    assert response.status_code == 503
    assert response.json()['detail'] == 'Database unavailable'
    assert db.rolled_back


def test_read_does_not_create_account(setup):
    client, db, _ = setup
    signin('alice')
    response = client.get('/preferences')
    assert response.headers['cache-control'] == 'no-store'
    assert db.statements == []


def test_write_identity_from_claims_without_ddl(setup):
    client, db, _ = setup
    signin('alice')
    assert client.post('/preferences', json={'timezone_override': None}).status_code == 200
    assert len(db.statements) == 1
    query, params = db.statements[0]
    assert 'INSERT INTO app.app_users' in query
    assert params == {'user_id': 'alice'}
    assert 'CREATE' not in query
