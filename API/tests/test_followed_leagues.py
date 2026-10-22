import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError
from API import app as api

class DB:
    def commit(self): self.committed = True
    def rollback(self): self.rolled_back = True

@pytest.fixture
def setup(monkeypatch):
    db, store = DB(), {}
    api.app.dependency_overrides[api.get_db] = lambda: db
    monkeypatch.setattr(api, 'ensure_account_user', lambda *args: None)
    monkeypatch.setattr(api, 'list_followed_leagues', lambda db, uid: store.get(uid, []))
    def replace(db, uid, ids):
        if any(i not in (4, 5) for i in ids): raise ValueError('One or more leagues are unavailable')
        store[uid] = sorted(ids)
    monkeypatch.setattr(api, 'replace_followed_leagues', replace)
    yield TestClient(api.app), db, store
    api.app.dependency_overrides.clear()

def signin(uid):
    api.app.dependency_overrides[api.get_current_user_claims] = lambda: {'sub':uid}

def test_auth(setup):
    c,_,_=setup
    assert c.get('/account/leagues').status_code==401
    assert c.put('/account/leagues',json={'league_ids':[4]}).status_code==401
    assert c.get('/account/leagues/directory').status_code==401

def test_save_clear_and_isolation(setup):
    c,db,store=setup;signin('alice')
    assert c.get('/account/leagues').json()['data']['league_ids']==[]
    for _ in range(2): assert c.put('/account/leagues',json={'league_ids':[5,4]}).json()['data']['league_ids']==[4,5]
    signin('bob');assert c.get('/account/leagues').json()['data']['league_ids']==[]
    signin('alice');assert c.put('/account/leagues',json={'league_ids':[]}).json()['data']['league_ids']==[]
    assert db.committed

@pytest.mark.parametrize('body',[{}, {'league_ids':[True]}, {'league_ids':['4']}, {'league_ids':[4.0]}, {'league_ids':[0]}, {'league_ids':[-1]}, {'league_ids':[4,4]}, {'league_ids':[4],'user_id':'bob'}, {'league_ids':[4]*201}])
def test_rejects_invalid_request(setup,body):
    c,_,store=setup;signin('alice')
    assert c.put('/account/leagues',json=body).status_code==422
    assert not store

def test_invalid_league_preserves_previous(setup):
    c,db,store=setup;signin('alice');store['alice']=[4]
    assert c.put('/account/leagues',json={'league_ids':[999]}).status_code==422
    assert store['alice']==[4] and db.rolled_back

def test_db_error(setup,monkeypatch):
    c,db,_=setup;signin('alice')
    def fail(*args):raise SQLAlchemyError('private configuration')
    monkeypatch.setattr(api,'replace_followed_leagues',fail)
    r=c.put('/account/leagues',json={'league_ids':[4]})
    assert r.status_code==503 and 'private' not in r.text and db.rolled_back
