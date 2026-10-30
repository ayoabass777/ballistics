from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError
from API import app as api


class DB:
    def __init__(self): self.statements = []
    def execute(self, query, params): self.statements.append((str(query), params))
    def commit(self): self.committed = True
    def rollback(self): self.rolled_back = True


@pytest.fixture
def setup(monkeypatch):
    db = DB()
    stored, enabled = [], {}
    api.app.dependency_overrides[api.get_db] = lambda: db
    monkeypatch.setattr(api, 'usage_events_enabled', lambda db, uid: enabled.get(uid, True))
    monkeypatch.setattr(api, 'insert_events', lambda db, uid, events: stored.extend((uid, e) for e in events))
    def save(db, uid, value):
        enabled[uid] = value
        return value
    monkeypatch.setattr(api, 'save_usage_events_setting', save)
    yield TestClient(api.app), db, stored, enabled
    api.app.dependency_overrides.clear()


def signin(uid):
    api.app.dependency_overrides[api.get_current_user_claims] = lambda: {'sub': uid}


def event(name='match_opened', props=None, **extra):
    return {'event_id': str(uuid4()), 'session_id': str(uuid4()), 'event_name': name,
            'props': {'fixture_id': 7, 'after_kickoff': False} if props is None else props,
            'occurred_at': datetime.now(timezone.utc).isoformat(), **extra}


def test_requires_auth(setup):
    client, *_ = setup
    assert client.post('/account/events', json={'events': [event()]}).status_code == 401
    assert client.post('/preferences/usage-events', json={'enabled': False}).status_code == 401


def test_records_batch_under_token_identity(setup):
    client, db, stored, _ = setup
    signin('alice')
    response = client.post('/account/events', json={'events': [event(), event('onboarding_finished', {'outcome': 'skipped', 'league_count': 0})]})
    assert response.status_code == 200
    assert response.json()['data'] == {'accepted': 2, 'enabled': True}
    assert response.headers['cache-control'] == 'no-store'
    assert [uid for uid, _ in stored] == ['alice', 'alice']
    assert 'INSERT INTO app.app_users' in db.statements[0][0]


@pytest.mark.parametrize('bad', [
    event('page_viewed', {}),
    event(props={'fixture_id': 7, 'email': 'a@b.c'}),
    event(props={'fixture_id': -1}),
    event(props={'fixture_id': True}),
    event('session_started', {'entry_route': 'https://evil.test/x', 'device': 'mobile'}),
    event('tutorial_dismissed', {'tutorial_id': 'unknown', 'action': 'skip'}),
    event(user_id='bob'),
    {**event(), 'occurred_at': '2026-10-26T12:00:00'},
    {**event(), 'occurred_at': (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()},
    {**event(), 'event_id': 'not-a-uuid'},
    event('request_failed', {'api': '/api/research/discover?q=arsenal', 'method': 'GET', 'status': 503, 'reason': 'http'}),
    event('request_failed', {'api': 'https://evil.test/api/x', 'method': 'GET', 'status': 503, 'reason': 'http'}),
    event('request_failed', {'api': '/api/account/leagues', 'method': 'GET', 'status': 700, 'reason': 'http'}),
    event('client_error', {'route': '/matches', 'kind': 'error', 'message': 'boom'}),
    event('search_used', {'page': 'matches', 'result_count': 2, 'query_length': 7, 'query': 'arsenal'}),
    event('filter_changed', {'page': 'matches', 'filter': 'team', 'result_count': 2}),
    event('page_speed', {'route': '/matches', 'metric': 'cls', 'value_ms': 1, 'rating': 'good'}),
    event('page_speed', {'route': '/matches', 'metric': 'lcp', 'value_ms': 1.5, 'rating': 'good'}),
])
def test_rejects_events_outside_allow_list(setup, bad):
    client, _, stored, _ = setup
    signin('alice')
    assert client.post('/account/events', json={'events': [bad]}).status_code == 422
    assert not stored


@pytest.mark.parametrize('good', [
    event('request_failed', {'api': '/api/research/match/[id]', 'method': 'GET', 'status': 0, 'reason': 'network'}),
    event('client_error', {'route': '/match/[id]', 'kind': 'unhandled_rejection'}),
    event('retry_clicked', {'api': '/api/account/leagues'}),
    event('search_used', {'page': 'all', 'result_count': 0, 'query_length': 4}),
    event('filter_changed', {'page': 'discover', 'filter': 'evidence_filter', 'result_count': 12}),
    event('page_speed', {'route': '/matches', 'metric': 'inp', 'value_ms': 184, 'rating': 'good'}),
])
def test_accepts_error_search_and_speed_events(setup, good):
    client, _, stored, _ = setup
    signin('alice')
    assert client.post('/account/events', json={'events': [good]}).status_code == 200
    assert len(stored) == 1


@pytest.mark.parametrize('events', [[], [event() for _ in range(21)]])
def test_batch_size_bounds(setup, events):
    client, _, stored, _ = setup
    signin('alice')
    assert client.post('/account/events', json={'events': events}).status_code == 422
    assert not stored


def test_opted_out_member_stores_nothing(setup):
    client, _, stored, enabled = setup
    signin('alice')
    response = client.post('/preferences/usage-events', json={'enabled': False})
    assert response.json()['data'] == {'usage_events_enabled': False}
    assert enabled == {'alice': False}
    response = client.post('/account/events', json={'events': [event()]})
    assert response.status_code == 200
    assert response.json()['data'] == {'accepted': 0, 'enabled': False}
    assert not stored
    signin('bob')
    assert client.post('/account/events', json={'events': [event()]}).json()['data']['accepted'] == 1


@pytest.mark.parametrize('body', [{}, {'enabled': 'no'}, {'enabled': False, 'user_id': 'bob'}])
def test_invalid_setting(setup, body):
    client, _, _, enabled = setup
    signin('alice')
    assert client.post('/preferences/usage-events', json=body).status_code == 422
    assert not enabled


def test_db_error_rolls_back(setup, monkeypatch):
    client, db, _, _ = setup
    signin('alice')
    def fail(*args): raise SQLAlchemyError('private connection information')
    monkeypatch.setattr(api, 'insert_events', fail)
    response = client.post('/account/events', json={'events': [event()]})
    assert response.status_code == 503
    assert response.json()['detail'] == 'Database unavailable'
    assert db.rolled_back
